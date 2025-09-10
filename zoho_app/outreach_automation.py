"""
Outreach Automation Module

This module handles automated email outreach to companies with matched candidates.
It implements the following workflow:

1. Group top 3 candidates by intern role
2. Get company contact emails (layout_name="partner")
3. Get partnership specialist email for sender
4. Check urgency based on visa requirements and start dates
5. Send emails with candidate information and refined bios
6. Implement follow-up workflow (48h intervals):
   - Follow-up 1: after 48h
   - Follow-up 2: after 96h (48h after follow-up 1)
   - Follow-up 3: after 144h (48h after follow-up 2)
   - Move to next roles: after 192h (48h after follow-up 3)
"""

import json
import logging
import smtplib
import uuid
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import make_msgid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from django.db import transaction
from django.db.models import Q, Count
from django.utils import timezone
from django.conf import settings
from zoho_app.models import Contact, InternRole, JobMatch, Account, Document, OutreachLog, EmailLimiter, FollowUpTask, CandidateOutreachHistory
from zoho.api_client import ZohoClient
import os
import requests

# Set up logging
logger = logging.getLogger(__name__)

try:
    import openai
    OPENAI_AVAILABLE = True
    if hasattr(settings, 'OPENAI_API_KEY'):
        openai.api_key = settings.OPENAI_API_KEY
except ImportError:
    OPENAI_AVAILABLE = False



class OutreachAutomation:

    def __init__(self):
        self.email_templates = {
            'initial': self._get_initial_email_template(),
            'follow_up': self._get_follow_up_template(),
            'final': self._get_final_template(),
        }
        self.urgent_email_templates = {
            'initial': self._get_urgent_initial_email_template(),
            'follow_up': self._get_urgent_follow_up_template(),
            'final': self._get_urgent_final_template(),
        }

    def run_urgent_outreach_batch(self, max_roles: int = None) -> Dict[str, Any]:
        """
        Run batch outreach for all roles with urgent candidates (urgency condition met)
        Includes follow-up workflow as in the normal flow
        """
        try:
            logger.info("Starting batch urgent outreach automation")

            # Get urgent candidates grouped by role
            role_candidates = self.get_urgent_candidates_by_role()

            if not role_candidates:
                logger.info("No urgent role candidates found for outreach")
                return {'status': 'completed', 'roles_processed': 0}

            results = []
            processed_count = 0

            # Limit processing to max_roles if specified
            for role_id, candidates in role_candidates.items():
                if max_roles and processed_count >= max_roles:
                    break

                if len(candidates) == 0:
                    continue

                logger.info(f"Processing urgent outreach for role {role_id} with {len(candidates)} candidates")

                result = self.process_outreach_for_role(role_id, candidates)
                result['role_id'] = role_id
                results.append(result)

                processed_count += 1

            successful = len([r for r in results if r['status'] == 'success'])

            logger.info(f"Batch urgent outreach completed: {successful}/{len(results)} roles processed successfully")

            return {
                'status': 'completed',
                'roles_processed': len(results),
                'successful': successful,
                'failed': len(results) - successful,
                'results': results
            }

        except Exception as e:
            logger.error(f"Error in batch urgent outreach: {e}")
            return {'status': 'failed', 'error': str(e)}

    def get_urgent_candidates_by_role(self, limit_per_role: int = 3) -> Dict[str, List[Dict]]:
        """
        Group top urgency candidates by intern role based on match scores
        Only includes candidates who meet the urgency condition and have not already been pitched to the role.
        Ensures each candidate appears only once across all roles' top 3 lists.
        Returns:
            Dictionary with intern_role_id as key and list of top candidates as value
        """
        try:
            matches = JobMatch.objects.filter(
                status='active',
                match_score__gte=0.2
            ).order_by('intern_role_id', '-match_score')

            role_candidates = {}
            used_candidate_ids = set()

            for match in matches:
                role_id = match.intern_role_id
                candidate_id = match.contact_id
                urgency_contact = Contact.objects.filter(id=match.contact_id).first()
                if not urgency_contact or not self.check_urgency(urgency_contact):
                    # Only include urgency candidates
                    continue

                # Check if this candidate has already been pitched to this role
                if CandidateOutreachHistory.objects.filter(
                    contact_id=candidate_id, intern_role_id=role_id
                ).exists():
                    continue

                # Ensure candidate is not already used in another role's top 3
                if candidate_id in used_candidate_ids:
                    continue

                if role_id not in role_candidates:
                    role_candidates[role_id] = []

                # Only add if we haven't reached the limit for this role
                if len(role_candidates[role_id]) < limit_per_role:
                    try:
                        contact = Contact.objects.get(id=match.contact_id)
                        candidate_info = {
                            'contact_id': contact.id,
                            'contact': contact,
                            'match_score': match.match_score,
                            'full_name': contact.full_name,
                            'email': contact.email,
                            'start_date': contact.start_date,
                            'end_date': contact.end_date,
                            'student_bio': contact.student_bio,
                            'requires_visa': contact.requires_a_visa,
                            'partnership_specialist_id': contact.partnership_specialist_id,
                            'skills': contact.skills,
                            'university_name': contact.university_name,
                            'graduation_date': contact.graduation_date,
                            'industry_choice_1': contact.industry_choice_1,
                            'industry_choice_2': contact.industry_choice_2,
                            'industry_choice_3': contact.industry_choice_3,
                        }
                        role_candidates[role_id].append(candidate_info)
                        used_candidate_ids.add(candidate_id)
                    except Contact.DoesNotExist:
                        continue

            filtered_role_candidates = {k: v for k, v in role_candidates.items() if v}
            logger.info(f"Found urgency candidates for {len(filtered_role_candidates)} roles")
            return filtered_role_candidates
        except Exception as e:
            logger.error(f"Error getting urgent candidates by role: {e}")
            return {}
        
    def get_top_candidates_by_role(self, limit_per_role: int = 3) -> Dict[str, List[Dict]]:
        """
        Group top candidates by intern role based on match scores
        Excludes candidates who have already been pitched to each role
        
        Returns:
            Dictionary with intern_role_id as key and list of top candidates as value
        """
        try:
            # Get all active job matches, grouped by intern role
            matches = JobMatch.objects.filter(
                status='active',
                match_score__gte=0.2
            ).order_by('intern_role_id', '-match_score')
            
            role_candidates = {}
            limit_per_candidate = {}
            for match in matches:
                role_id = match.intern_role_id
                candidate_id = match.contact_id
                urgency_contact = Contact.objects.filter(id=match.contact_id,student_status='ACTIVE: Placement').exclude(role_success_stage='Role Confirmed').first()
                if not urgency_contact or self.check_urgency(urgency_contact):
                    logger.info(f"Skipping candidate {candidate_id} for role {role_id} - urgency condition met")
                    continue

                # Check if this candidate has already been pitched to this role
                if CandidateOutreachHistory.objects.filter(
                    contact_id=candidate_id, intern_role_id=role_id
                ).exists():
                    logger.debug(f"Skipping candidate {candidate_id} for role {role_id} - already pitched")
                    continue

                if limit_per_candidate.get(match.contact_id, 0) > 3:
                    logger.info(f"Skipping candidate {candidate_id} for role {role_id} - reached limit")
                    continue

                if role_id not in role_candidates:
                    role_candidates[role_id] = []
                
                # Only add if we haven't reached the limit for this role
                if len(role_candidates[role_id]) < limit_per_role:
                    try:
                        contact = Contact.objects.get(id=match.contact_id)
                        
                        candidate_info = {
                            'contact_id': contact.id,
                            'contact': contact,
                            'match_score': match.match_score,
                            'full_name': contact.full_name,
                            'email': contact.email,
                            'start_date': contact.start_date,
                            'end_date': contact.end_date,
                            'student_bio': contact.student_bio,
                            'requires_visa': contact.requires_a_visa,
                            'partnership_specialist_id': contact.partnership_specialist_id,
                            'skills': contact.skills,
                            'university_name': contact.university_name,
                            'graduation_date': contact.graduation_date,
                            'industry_choice_1': contact.industry_choice_1,
                            'industry_choice_2': contact.industry_choice_2,
                            'industry_choice_3': contact.industry_choice_3,
                        }
                        
                        role_candidates[role_id].append(candidate_info)
                        if match.contact_id in limit_per_candidate:
                            limit_per_candidate[match.contact_id] += 1
                        else:
                            limit_per_candidate[match.contact_id] = 1

                    except Contact.DoesNotExist:
                        logger.warning(f"Contact {match.contact_id} not found for match {match.id}")
                        continue
            
            # Filter out roles with no candidates
            filtered_role_candidates = {k: v for k, v in role_candidates.items() if v}
            
            logger.info(f"Found candidates for {len(filtered_role_candidates)} roles (after filtering already pitched)")
            return filtered_role_candidates
            
        except Exception as e:
            logger.error(f"Error getting top candidates by role: {e}")
            return {}
    
    def get_company_contact_emails(self, intern_role_id: str) -> List[Dict[str, Any]]:
        """Get company partner contact emails"""
        try:
            role = InternRole.objects.get(id=intern_role_id)
            if not role.intern_company_id:
                logger.warning(f"No company ID found for intern role {intern_role_id}")
                return []

            contacts = (
                Contact.objects.filter(
                    account_id=role.intern_company_id,
                    layout_name__iexact="partner",
                    email__isnull=False,
                )
                .exclude(email="")
                .values("id", "email", "first_name", "last_name", "full_name", "title")
            )

            contact_list = list(contacts)
            logger.info(f"Found {len(contact_list)} partner contacts for company {role.intern_company_id}")
            return contact_list

        except InternRole.DoesNotExist:
            logger.error(f"Intern role {intern_role_id} not found")
        except Exception as e:
            logger.error(f"Error getting company contact emails for role {intern_role_id}: {e}", exc_info=True)

        return []
    
    def get_partnership_specialist_email(self, partnership_specialist_id: str) -> Optional[Dict[str, Any]]:
        """
        Get partnership specialist email from Zoho Users API
        
        Args:
            partnership_specialist_id: The user ID of the partnership specialist
        
        Returns:
            Partnership specialist contact information from Zoho API
        """
        if not partnership_specialist_id:
            return None

        try:
            client = ZohoClient()
            url = f"https://www.zohoapis.com/crm/v2/users/{partnership_specialist_id}"
            resp = client.session.get(url, headers=client.headers, timeout=client.timeout)
            resp.raise_for_status()

            for user in resp.json().get("users", []):
                if user.get("email") and user.get("status", "").lower() == "active":
                    return {
                        "id": user.get("id"),
                        "email": user.get("email"),
                        "first_name": user.get("first_name"),
                        "last_name": user.get("last_name"),
                        "full_name": user.get("full_name"),
                    }
        except Exception as e:
            logger.error(f"Error fetching Zoho user {partnership_specialist_id}: {e}")
        return None

    def check_urgency(self, contact: Contact) -> bool:
        """
        Check if outreach should be urgent based on visa requirements and start date
        
        Urgency criteria:
        - Visa = Yes & Start Date < 120 days from today
        - Visa = No & Start Date < 60 days from today
        
        Returns:
            True if urgent, False otherwise
        """
        try:
            if not contact.start_date:
                return False
            
            today = timezone.now().date()
            start_date = contact.start_date.date() if hasattr(contact.start_date, 'date') else contact.start_date
            days_until_start = (start_date - today).days
            
            requires_visa = contact.requires_a_visa
            
            if requires_visa and requires_visa.lower() == 'yes':
                # Visa required: urgent if start date < 120 days
                return days_until_start < 120
            elif requires_visa and requires_visa.lower() == 'no':
                # No visa required: urgent if start date < 60 days
                return days_until_start < 60
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking urgency for contact {contact.id}: {e}")
            return False
    
    def generate_message_id(self, email_type: str = 'outreach') -> str:
        """
        Generate a unique message ID for email tracking
        
        Args:
            email_type: Type of email (initial, follow_up, final)
            
        Returns:
            Unique message ID string
        """
        try:
            # Create unique message ID using timestamp, UUID, and email type
            timestamp = int(time.time() * 1000)  # Milliseconds timestamp
            unique_id = str(uuid.uuid4())[:8]  # Short UUID
            domain = "beyondacademy.com"  # Your domain
            
            message_id = f"<{email_type}-{timestamp}-{unique_id}@{domain}>"
            logger.debug(f"Generated message ID: {message_id}")
            return message_id
            
        except Exception as e:
            logger.error(f"Error generating message ID: {e}")
            # Fallback to simple UUID-based ID
            return f"<outreach-{uuid.uuid4()}@beyondacademy.com>"
    
    def generate_thread_id(self, role_id: str, company_id: str) -> str:
        """
        Generate a thread ID for grouping related emails
        
        Args:
            role_id: Intern role ID
            company_id: Company ID
            
        Returns:
            Thread ID string for email threading
        """
        try:
            # Create consistent thread ID based on role and company
            base_string = f"role-{role_id}-company-{company_id}"
            thread_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, base_string))[:12]
            thread_id = f"<thread-{thread_uuid}@beyondacademy.com>"
            
            logger.debug(f"Generated thread ID: {thread_id}")
            return thread_id
            
        except Exception as e:
            logger.error(f"Error generating thread ID: {e}")
            return f"<thread-{uuid.uuid4()}@beyondacademy.com>"
    
    def refine_candidate_bio_with_gpt(self, student_bio: str, contact_info: Dict) -> str:
        """
        Refine candidate bio using GPT for professional presentation
        """
        if not OPENAI_AVAILABLE or not student_bio:
            return student_bio or ""
        
        try:
            # Create a professional bio prompt
            prompt = f"""
            Please refine the following student bio for a professional outreach email to potential internship companies. 
            Make it concise, professional, and highlight relevant skills and experience.
            
            Student Information:
            - Name: {contact_info.get('full_name', 'N/A')}
            - University: {contact_info.get('university_name', 'N/A')}
            - Industry Interests: {', '.join(filter(None, [contact_info.get('industry_choice_1'), contact_info.get('industry_choice_2'), contact_info.get('industry_choice_3')]))}
            - Skills: {contact_info.get('skills', 'N/A')}
            
            Original Bio:
            {student_bio}
            
            Please provide a refined, professional bio in 2-3 sentences that would appeal to hiring managers:
            """
            
            response = openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional recruiter writing compelling candidate descriptions."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.7
            )
            
            refined_bio = response.choices[0].message.content.strip()
            
            # Clean up the response by removing common prefixes
            prefixes_to_remove = [
                "Refined Bio:",
                "Refined bio:",
                "Bio:",
                "Professional Bio:",
                "Here's the refined bio:",
                "Here is the refined bio:",
                "The refined bio:",
                "Refined:"
            ]
            
            for prefix in prefixes_to_remove:
                if refined_bio.startswith(prefix):
                    refined_bio = refined_bio[len(prefix):].strip()
                    break
            
            logger.info(f"Successfully refined bio for {contact_info.get('full_name')}")
            return refined_bio
            
        except Exception as e:
            logger.error(f"Error refining bio with GPT: {e}")
            return student_bio or ""
    
    def get_candidate_resume_path(self, contact_id: str) -> Optional[str]:
        """
        Get the file path to the candidate's most recent resume
        """
        try:
            # Get the most recent document for this contact
            document = Document.objects.filter(
                contact_id=contact_id,
                document_type__icontains='cv'
            ).order_by('-download_date').first()
            
            if document and os.path.exists(document.file_path):
                return document.file_path
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting resume path for contact {contact_id}: {e}")
            return None

    def create_outreach_email(self, role: InternRole, candidates: List[Dict], company_contacts: List[Dict], email_type: str = 'initial', parent_outreach_log: Optional[OutreachLog] = None, urgent: bool = False) -> Dict[str, Any]:
        """
        Create outreach email content with candidate information using new batch template format
        Includes message ID generation for tracking and threading
        Uses different templates for urgent and non-urgent candidates
        """
        try:
            # Choose template set
            templates = self.urgent_email_templates if urgent else self.email_templates
            template = templates.get(email_type, templates['initial'])

            # Generate message ID for this email
            message_id = self.generate_message_id(email_type)

            # Generate or reuse thread ID for email threading
            thread_id = ""
            in_reply_to = ""

            if email_type == 'initial':
                # For initial emails, create new thread
                thread_id = self.generate_thread_id(role.id, role.intern_company_id or "unknown")
            else:
                # For follow-ups, use parent's thread and reference parent message
                if parent_outreach_log:
                    thread_id = parent_outreach_log.thread_id or self.generate_thread_id(role.id, role.intern_company_id or "unknown")
                    in_reply_to = parent_outreach_log.message_id or ""
                else:
                    # Fallback if no parent found
                    thread_id = self.generate_thread_id(role.id, role.intern_company_id or "unknown")

            # Get industry from role's company account
            industry = ""
            try:
                if role.intern_company_id:
                    from zoho_app.models import Account
                    account = Account.objects.filter(id=role.intern_company_id).first()
                    if account:
                        # Try company_industry first, then industry
                        if account.company_industry:
                            industry = account.company_industry
                        elif account.industry:
                            industry = account.industry

                # Fallback to role title if no industry found
                if not industry and hasattr(role, 'role_title') and role.role_title:
                    industry = role.role_title
                # If still no industry found, leave blank
            except Exception as e:
                logger.warning(f"Could not get industry for role {role.id}: {e}")
                industry = ""

            # Get contact name from first company contact
            contact_name = ""
            if company_contacts and len(company_contacts) > 0:
                first_contact = company_contacts[0]
                contact_name = first_contact.get('full_name') or first_contact.get('first_name') or "there"

            # Get partnership specialist name from first candidate who has one
            partnership_specialist = next(
                (
                    self.get_partnership_specialist_email(c['partnership_specialist_id']).get('full_name')
                    for c in candidates
                    if c.get('partnership_specialist_id')
                    and self.get_partnership_specialist_email(c['partnership_specialist_id'])
                    and self.get_partnership_specialist_email(c['partnership_specialist_id']).get('full_name')
                ),
                "Beyond Academy Team"
            )

            # Prepare candidate information for initial email only
            candidate_sections = []
            attachments = []

            if email_type == 'initial':
                for candidate in candidates:
                    # For urgent, only one candidate per email (per your template)
                    if urgent:
                        # Get specific area within industry
                        specific_area = candidate.get("industry") or candidate.get("industry_choice_1") or candidate.get("industry_choice_2") or ""
                        start_date = candidate.get('start_date')
                        date_str = start_date.strftime('%B %Y') if start_date and hasattr(start_date, 'strftime') else str(start_date) if start_date else ""
                        duration_str = f" for {candidate['duration']}" if candidate.get('duration') else ""
                        availability_info = f"Availability: {date_str}{duration_str}" if date_str else ""
                        refined_bio = (
                            self.refine_candidate_bio_with_gpt(candidate['student_bio'], candidate)
                            if candidate.get('student_bio') else ""
                        )
                        candidate_info = f"""{industry} Intern – {candidate['full_name']}
{availability_info}
{refined_bio}"""
                        candidate_sections.append(candidate_info)
                        resume_path = self.get_candidate_resume_path(candidate['contact_id'])
                        if resume_path:
                            attachments.append({
                                'path': resume_path,
                                'name': f"{candidate['full_name']}_Resume.pdf"
                            })
                        break  # Only one candidate per urgent email
                    else:
                        # Non-urgent: batch multiple candidates
                        specific_area = candidate.get("industry") or candidate.get("industry_choice_1") or candidate.get("industry_choice_2") or ""
                        start_date = candidate.get('start_date')
                        date_str = start_date.strftime('%B %Y') if start_date and hasattr(start_date, 'strftime') else str(start_date) if start_date else ""
                        duration_str = f" for {candidate['duration']}" if candidate.get('duration') else ""
                        availability_info = f"Available: {date_str}{duration_str}" if date_str else ""
                        refined_bio = (
                            self.refine_candidate_bio_with_gpt(candidate['student_bio'], candidate)
                            if candidate.get('student_bio') else ""
                        )
                        specific_area_text = f" – Interested in {specific_area}" if specific_area else ""
                        candidate_info = f"""{candidate['full_name']}{specific_area_text}
{availability_info}
{refined_bio}"""
                        candidate_sections.append(candidate_info)
                        resume_path = self.get_candidate_resume_path(candidate['contact_id'])
                        if resume_path:
                            attachments.append({
                                'path': resume_path,
                                'name': f"{candidate['full_name']}_Resume.pdf"
                            })

            # Prepare email content
            subject = template['subject'].format(
                industry=industry,
                intern_name=candidates[0]['full_name'] if urgent and candidates else ""
            )
            if email_type != 'initial' and parent_outreach_log and parent_outreach_log.subject:
                subject = f"Re: {parent_outreach_log.subject}"
            elif email_type != 'initial':
                # Fallback for follow-ups if parent subject is not available
                base_subject = template['subject'].format(
                    industry=industry,
                    intern_name=candidates[0]['full_name'] if urgent and candidates else ""
                )
                subject = f"Re: {base_subject}"

            body = template['body'].format(
                industry=industry,
                contact_name=contact_name,
                candidates_info='\n\n'.join(candidate_sections) if candidate_sections else "",
                partnership_specialist=partnership_specialist,
                intern_name=candidates[0]['full_name'] if urgent and candidates else ""
            )

            return {
                'subject': subject,
                'body': body,
                'attachments': attachments,
                'recipients': [contact['email'] for contact in company_contacts],
                'message_id': message_id,
                'thread_id': thread_id,
                'in_reply_to': in_reply_to
            }

        except Exception as e:
            logger.error(f"Error creating outreach email: {e}")
            return {}

    def _get_urgent_initial_email_template(self) -> Dict[str, str]:
        return {
            'subject': 'Outstanding Intern Available – {industry}',
            'body': '''Hi {contact_name},

I’m reaching out to introduce an outstanding {industry} intern who could make a real impact on your team. As you are aware, partnering with Beyond Academy is completely free, and we provide motivated, pre-vetted talent ready to contribute from day one.

Do you think you might have a suitable opportunity available?

Here’s a snapshot of the intern’s profile:

{candidates_info}

As start dates are approaching, we’d like to move quickly. Would you be open to scheduling interviews?

Many thanks
{partnership_specialist}
Beyond Academy
https://beyondacademy.com/

Tokyo - Seoul - Bangkok - Sydney - London - Dublin - Berlin - Barcelona - Paris - Stockholm - Amsterdam - New York - Toronto - San Francisco'''
        }

    def _get_urgent_follow_up_template(self) -> Dict[str, str]:
        return {
            'subject': 'Following up – Outstanding {industry} Intern Available',
            'body': '''Hi {contact_name},

Just following up on my previous email about an exceptional {industry} intern we’d love to connect with your team. Since start dates are fast approaching, we’re keen to move quickly to secure a suitable placement.

Would you be open to a brief call or setting up interviews to explore whether this opportunity could be a good fit?

Many thanks
{partnership_specialist}
Beyond Academy
https://beyondacademy.com/

Tokyo - Seoul - Bangkok - Sydney - London - Dublin - Berlin - Barcelona - Paris - Stockholm - Amsterdam - New York - Toronto - San Francisco'''
        }

    def _get_urgent_final_template(self) -> Dict[str, str]:
        return {
            'subject': 'Last call – {industry} Intern Availability',
            'body': '''Hi {contact_name},

I wanted to make one final check-in regarding the {industry} intern I introduced earlier. They are eager to contribute their skills and experience to a forward-thinking organisation, and we’d love to see if your team could be the right fit.

If now isn’t the right time, no worries, but if you’d like to explore this, we’d be happy to set up interviews before placements are finalised.

Looking forward to your reply.
Many thanks
{partnership_specialist}
Beyond Academy
https://beyondacademy.com/

Tokyo - Seoul - Bangkok - Sydney - London - Dublin - Berlin - Barcelona - Paris - Stockholm - Amsterdam - New York - Toronto - San Francisco'''
        }
    
    def send_email(self, 
                  email_content: Dict[str, Any], 
                  sender_email: str,
                  sender_name: str = None) -> bool:
        """
        Send the outreach email using Django's email backend (Gmail SMTP)
        """
        try:
            from django.core.mail import EmailMessage
            from django.conf import settings
            import os
            
            # Validate Gmail configuration
            if not settings.EMAIL_HOST_USER or not settings.EMAIL_HOST_PASSWORD:
                logger.error("Gmail SMTP configuration missing. Please set GMAIL_EMAIL and GMAIL_APP_PASSWORD in .env file")
                return False
            
            # Create email message
            subject = email_content.get('subject', 'No Subject')
            body = email_content.get('body', '')
            recipients = email_content.get('recipients', [])
            attachments = email_content.get('attachments', [])
            
            if not recipients:
                logger.error("No recipients specified for email")
                return False
            
            # Create EmailMessage instance
            email = EmailMessage(
                subject=subject,
                body=body,
                from_email=f"{sender_name} <{sender_email}>" if sender_name else settings.EMAIL_HOST_USER,
                to=["prabhat.scaleupally@gmail.com"],
                # to=recipients,
                reply_to=[sender_email,"molly@beyondacademy.com"] if sender_email != settings.EMAIL_HOST_USER else None
            )
            
            # Add message tracking headers
            message_id = email_content.get('message_id')
            thread_id = email_content.get('thread_id')
            in_reply_to = email_content.get('in_reply_to')
            
            if message_id:
                email.extra_headers['Message-ID'] = message_id
                logger.info(f"Message-ID: {message_id}")
            
            if thread_id:
                email.extra_headers['Thread-Index'] = thread_id
                logger.info(f"Thread-Index: {thread_id}")
            
            if in_reply_to:
                email.extra_headers['In-Reply-To'] = in_reply_to
                email.extra_headers['References'] = in_reply_to
                logger.info(f"In-Reply-To: {in_reply_to}")
            
            # Add attachments
            for attachment in attachments:
                attachment_path = attachment.get('path')
                attachment_name = attachment.get('name', os.path.basename(attachment_path))
                
                if attachment_path and os.path.exists(attachment_path):
                    try:
                        email.attach_file(attachment_path, mimetype=None)
                        logger.info(f"Attached file: {attachment_name}")
                    except Exception as e:
                        logger.warning(f"Failed to attach file {attachment_path}: {e}")
                else:
                    logger.warning(f"Attachment file not found: {attachment_path}")
            
            # Send the email
            try:
                email.send(fail_silently=False)
                logger.info(f"Email sent successfully to {len(recipients)} recipients")
                logger.info(f"Subject: {subject}")
                logger.info(f"Recipients: {', '.join(recipients)}")
                logger.info(f"Attachments: {len(attachments)}")
                logger.info(f"Sender: {sender_name} <{settings.EMAIL_HOST_USER}>")
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to send email: {e}")
                # Log detailed error information
                logger.error(f"SMTP Host: {settings.EMAIL_HOST}")
                logger.error(f"SMTP Port: {settings.EMAIL_PORT}")
                logger.error(f"Email User: {settings.EMAIL_HOST_USER}")
                return False
            
        except ImportError as e:
            logger.error(f"Django email import error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False
    
    def process_outreach_for_role(self, intern_role_id: str, candidates: List[Dict]) -> Dict[str, Any]:
        """
        Process outreach for a specific role with its top candidates
        """
        try:
            # Get the intern role
            role = InternRole.objects.get(id=intern_role_id)
            # Check urgency for any candidate
            is_urgent = any(self.check_urgency(candidate['contact']) for candidate in candidates)
            
            if not is_urgent:
                # Check if we can send email to this company (weekly limit)
                if role.intern_company_id and not self.can_send_email_to_company(role.intern_company_id):
                    logger.info(f"Email limit reached for company {role.intern_company_id}, skipping role {intern_role_id}")
                    return {'status': 'skipped', 'reason': 'email_limit_reached'}
            
            # Get company contact emails
            company_contacts = self.get_company_contact_emails(intern_role_id)
            if not company_contacts:
                logger.warning(f"No partner contacts found for role {intern_role_id}")
                return {'status': 'skipped', 'reason': 'no_company_contacts'}
            
            
            
            # Determine sender (partnership specialist)
            sender_info = None
            for candidate in candidates:
                if candidate.get('partnership_specialist_id'):
                    sender_info = self.get_partnership_specialist_email(candidate['partnership_specialist_id'])
                    if sender_info:
                        break
            
            if not sender_info:
                logger.warning(f"No partnership specialist found for candidates in role {intern_role_id}")
                # Use a default sender or skip
                return {'status': 'skipped', 'reason': 'no_partnership_specialist'}
            
            # Create email content
            email_content = self.create_outreach_email(role, candidates, company_contacts)
            if not email_content:
                return {'status': 'failed', 'reason': 'email_creation_failed'}
            
            # Extract message tracking information
            message_id = email_content.get('message_id')
            thread_id = email_content.get('thread_id')
            in_reply_to = email_content.get('in_reply_to')
            
            # Send email
            success = self.send_email(
                email_content, 
                sender_info['email'],
                sender_info['full_name']
            )
            
            if success:
                # Log the outreach in database
                self.log_outreach_sent(intern_role_id, candidates, company_contacts, is_urgent, 
                                     email_content, sender_info, role, message_id, thread_id, in_reply_to)
                
                return {
                    'status': 'success',
                    'role_id': intern_role_id,
                    'candidates_count': len(candidates),
                    'recipients_count': len(company_contacts),
                    'is_urgent': is_urgent,
                    'sender': sender_info['email']
                }
            else:
                return {'status': 'failed', 'reason': 'email_send_failed'}
                
        except InternRole.DoesNotExist:
            logger.error(f"Intern role {intern_role_id} not found")
            return {'status': 'failed', 'reason': 'role_not_found'}
        except Exception as e:
            logger.error(f"Error processing outreach for role {intern_role_id}: {e}")
            return {'status': 'failed', 'reason': str(e)}
    
    def run_batch_outreach(self, max_roles: int = None) -> Dict[str, Any]:
        """
        Run batch outreach for all roles with top candidates
        """
        try:
            logger.info("Starting batch outreach automation")
            
            # Get top candidates grouped by role
            role_candidates = self.get_top_candidates_by_role()
            
            if not role_candidates:
                logger.info("No role candidates found for outreach")
                return {'status': 'completed', 'roles_processed': 0}
            
            results = []
            processed_count = 0

            # Limit processing to max_roles if specified
            for role_id, candidates in role_candidates.items():
                if max_roles and processed_count >= max_roles:
                    break
                
                if len(candidates) == 0:
                    continue
                
                logger.info(f"Processing outreach for role {role_id} with {len(candidates)} candidates")
                
                result = self.process_outreach_for_role(role_id, candidates)
                result['role_id'] = role_id
                results.append(result)
                
                processed_count += 1
            
            successful = len([r for r in results if r['status'] == 'success'])
            
            logger.info(f"Batch outreach completed: {successful}/{len(results)} roles processed successfully")
            
            return {
                'status': 'completed',
                'roles_processed': len(results),
                'successful': successful,
                'failed': len(results) - successful,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Error in batch outreach: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def log_outreach_sent(self, role_id: str, candidates: List[Dict], recipients: List[Dict], is_urgent: bool,
                         email_content: Dict[str, Any], sender_info: Dict[str, Any], role: InternRole,
                         message_id: str = None, thread_id: str = None, in_reply_to: str = None, 
                         parent_outreach_log: 'OutreachLog' = None):
        """
        Log outreach email sent for tracking and follow-up purposes
        """
        try:
            # Create outreach log entry
            outreach_log = OutreachLog.objects.create(
                intern_role_id=role_id,
                role_title=role.role_title or role.name,
                company_id=role.intern_company_id,
                company_name=role.intern_company_name,
                subject=email_content.get('subject', ''),
                email_type='initial',
                sender_email=sender_info['email'],
                sender_name=sender_info['full_name'],
                recipients=json.dumps([r['email'] for r in recipients]),
                candidate_ids=json.dumps([c['contact_id'] for c in candidates]),
                candidates_count=len(candidates),
                is_urgent=is_urgent,
                is_sent=True,
                sent_at=timezone.now(),
                message_id=message_id,
                thread_id=thread_id,
                in_reply_to=in_reply_to,
                parent_outreach_log=parent_outreach_log
            )
            
            # Create candidate outreach history records
            for candidate in candidates:
                try:
                    # Determine cycle number (how many times this candidate has been pitched to this role)
                    existing_cycles = CandidateOutreachHistory.objects.filter(
                        contact_id=candidate['contact_id'],
                        intern_role_id=role_id
                    ).count()
                    
                    cycle_number = existing_cycles + 1
                    
                    CandidateOutreachHistory.objects.create(
                        contact_id=candidate['contact_id'],
                        intern_role_id=role_id,
                        outreach_log=outreach_log,
                        cycle_number=cycle_number,
                        initial_outreach_date=timezone.now(),
                        status='active'
                    )
                    
                    logger.info(f"Created outreach history for candidate {candidate['contact_id']} -> role {role_id} (Cycle {cycle_number})")
                    
                except Exception as e:
                    logger.error(f"Error creating outreach history for candidate {candidate['contact_id']}: {e}")
                    continue
            
            # Schedule follow-up tasks
            self.schedule_follow_ups(outreach_log)
            
            # Update email limiter for company
            self.update_email_limiter(role.intern_company_id, role.intern_company_name)
            
            logger.info(f"Outreach logged: Role {role_id}, Log ID {outreach_log.id}")
            
        except Exception as e:
            logger.error(f"Error logging outreach: {e}")
    
    def schedule_follow_ups(self, outreach_log: OutreachLog):
        """
        Schedule follow-up tasks for an outreach email
        """
        try:
            now = timezone.now()
            
            # Schedule first follow-up after 48 hours
            follow_up_date = now + timedelta(hours=48)
            FollowUpTask.objects.create(
                outreach_log=outreach_log,
                follow_up_type='follow_up',
                scheduled_date=follow_up_date
            )
            
            # Schedule final follow-up after 96 hours (48h after first follow-up)
            final_date = now + timedelta(hours=96)
            FollowUpTask.objects.create(
                outreach_log=outreach_log,
                follow_up_type='final',
                scheduled_date=final_date
            )
            
            # Schedule move to next roles task after 144 hours (48h after final follow-up)
            move_to_next_date = now + timedelta(hours=144)
            FollowUpTask.objects.create(
                outreach_log=outreach_log,
                follow_up_type='move_to_next',
                scheduled_date=move_to_next_date
            )
            
            # Update outreach log with next follow-up date
            outreach_log.next_follow_up_date = follow_up_date
            outreach_log.save()
            
            logger.info(f"Follow-up tasks scheduled for outreach log {outreach_log.id}")
            
        except Exception as e:
            logger.error(f"Error scheduling follow-ups: {e}")
    
    def update_email_limiter(self, company_id: str, company_name: str):
        """
        Update email limiter to track weekly email frequency
        """
        try:
            now = timezone.now()
            today = now.date()
            
            # Calculate start of current week (Monday)
            days_since_monday = today.weekday()
            week_start = today - timedelta(days=days_since_monday)
            
            limiter, created = EmailLimiter.objects.get_or_create(
                company_id=company_id,
                defaults={
                    'company_name': company_name,
                    'last_email_date': now,
                    'emails_sent_this_week': 1,
                    'week_start_date': week_start
                }
            )
            
            if not created:
                # Check if we're in a new week
                if limiter.week_start_date < week_start:
                    limiter.week_start_date = week_start
                    limiter.emails_sent_this_week = 1
                else:
                    limiter.emails_sent_this_week += 1
                
                limiter.last_email_date = now
                limiter.save()
            
            logger.info(f"Email limiter updated: {company_name} - {limiter.emails_sent_this_week} emails this week")
            
        except Exception as e:
            logger.error(f"Error updating email limiter: {e}")
    
    def can_send_email_to_company(self, company_id: str) -> bool:
        """
        Check if we can send an email to a company based on weekly limits
        """
        try:
            today = timezone.now().date()
            days_since_monday = today.weekday()
            week_start = today - timedelta(days=days_since_monday)
            
            limiter = EmailLimiter.objects.filter(company_id=company_id).first()
            
            if not limiter:
                return True  # No previous emails sent
            
            # If it's a new week, we can send
            if limiter.week_start_date < week_start:
                return True
            
            # Check if we've exceeded weekly limit (default: 1 email per week)
            weekly_limit = 1
            return limiter.emails_sent_this_week < weekly_limit
            
        except Exception as e:
            logger.error(f"Error checking email limit for company {company_id}: {e}")
            return True  # Default to allowing email if there's an error
    
    def _get_initial_email_template(self) -> Dict[str, str]:
        """Get initial outreach email template"""
        return {
            'subject': 'Outstanding Interns Available – {industry}',
            'body': '''Hi {contact_name},

I'm reaching out to introduce several outstanding {industry} interns who could make a real impact on your team. As you are aware, partnering with Beyond Academy is completely free, and we provide motivated, pre-vetted talent ready to contribute from day one.

Do you think you might have suitable opportunities available?

Here's a snapshot of the interns:

{candidates_info}

All of these interns are adaptable, proactive, and eager to contribute innovative ideas to your team. With start dates approaching, we'd like to move quickly. Would you be open to scheduling interviews to explore potential fits?

Many thanks
{partnership_specialist}
Beyond Academy
https://beyondacademy.com/'''
        }
    
    def _get_follow_up_template(self) -> Dict[str, str]:
        """Get follow-up email template"""
        return {
            'subject': 'Following up – Outstanding {industry} Interns Available',
            'body': '''Hi {contact_name},

I'm following up on my previous email introducing a few exceptional {industry} interns who could bring real value to your team. With start dates approaching, we're keen to move quickly to connect them with suitable opportunities.

Would you be open to a brief call or setting up interviews to explore which interns could be the best fit for your team?

Many thanks
{partnership_specialist}
Beyond Academy
https://beyondacademy.com/'''
        }
    
    def _get_final_template(self) -> Dict[str, str]:
        """Get final follow-up email template"""
        return {
            'subject': 'Last Call – {industry} Interns Availability',
            'body': '''Hi {contact_name},

I wanted to make one final check-in regarding the {industry} interns I introduced earlier. They are eager to contribute their skills and enthusiasm to a forward-thinking organisation, and we'd love to see if your team could be the right fit.

If now isn't the right time, no worries, but if you'd like to explore this, we'd be happy to set up interviews before placements are finalised.

Looking forward to your reply.

Many thanks
{partnership_specialist}
Beyond Academy
https://beyondacademy.com/'''
        }


def run_outreach_automation(dry_run: bool = False, max_roles: int = None) -> Dict[str, Any]:
    """
    Main function to run outreach automation
    
    Args:
        dry_run: If True, simulate the process without sending emails
        max_roles: Maximum number of roles to process (for testing)
    
    Returns:
        Results dictionary
    """
    logger.info(f"Starting outreach automation (dry_run={dry_run})")
    
    automation = OutreachAutomation()
    
    if dry_run:
        # In dry run mode, just return the candidates that would be processed (both normal and urgent)
        top_role_candidates = automation.get_top_candidates_by_role()
        urgent_role_candidates = automation.get_urgent_candidates_by_role()
        return {
            'status': 'dry_run_completed',
            'roles_found': len(top_role_candidates),
            'total_candidates': sum(len(candidates) for candidates in top_role_candidates.values()),
            'role_candidates': top_role_candidates,
            'urgent_roles_found': len(urgent_role_candidates),
            'urgent_total_candidates': sum(len(candidates) for candidates in urgent_role_candidates.values()),
            'urgent_role_candidates': urgent_role_candidates
        }
    else:
        normal_result = automation.run_batch_outreach(max_roles=max_roles)
        urgent_result = automation.run_urgent_outreach_batch(max_roles=max_roles)
        return {
            'normal_outreach': normal_result,
            'urgent_outreach': urgent_result
        }
