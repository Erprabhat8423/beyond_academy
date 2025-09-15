"""
Job Matching Service

This module implements the job matching logic between contacts (students) and intern roles.
Matching criteria:
- Role Tags = Primary/Secondary interest (industry fields)
- Student Location = Job Location
- Work Policy = Hybrid/In-office (unless student is remote, then remote only)
- Skills matching (optional enhancement)
- Start date priority: +0.10 score bonus if candidate's start date falls within 2 weeks before confirmed role end date

Enhanced filtering criteria:
1. Exclude companies with is_dnc = True
2. Exclude companies with empty, today, or past follow_up_date (include only future dates)
3. Exclude companies where Intern-to-employee ratio > 1:4 (based on confirmed deals vs employee count)
4. Exclude companies with more than 3 active deals (Scheduling/Interviewing/Pending Outcome stages)
"""
import json
import logging
import os
import time
from typing import List, Dict, Any, Tuple
from datetime import datetime, date
from django.db import transaction
from django.db.models import Q, Count
from django.utils import timezone
from difflib import SequenceMatcher
from zoho_app.models import Contact, InternRole, JobMatch, Skill, Account, Deal, RoleDealSync
import requests
import re
from zoho import auth
from django.db.models import Q
from django.conf import settings
from django.core.mail import send_mail

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

# Set up logging
logger = logging.getLogger(__name__)


class JobMatcher:
    """
    Job matching service for matching contacts with intern roles
    """
    
    def __init__(self):
        # Initialize OpenAI client for industry matching
        self.client = None
        if OPENAI_AVAILABLE:
            openai_api_key = os.getenv('OPENAI_API_KEY')
            if openai_api_key:
                try:
                    self.client = openai.OpenAI(api_key=openai_api_key)
                    logger.info("OpenAI client initialized for industry matching")
                except Exception as e:
                    logger.warning(f"Failed to initialize OpenAI client: {e}")
            else:
                logger.warning("OPENAI_API_KEY not found in environment variables")
        else:
            logger.warning("OpenAI package not available. Using fallback matching method.")
    
    
    def validate_api_access(self, intern_role_id: str) -> bool:
        """
        Validate if we can access the role and its basic information
        """
        try:
            access_token = auth.get_access_token()
            if not access_token:
                return False
            
            # Test basic role access first
            api_url = f"https://www.zohoapis.com/crm/v2/Intern_Roles/{intern_role_id}"
            headers = {
                'Authorization': f'Zoho-oauthtoken {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(api_url, headers=headers, timeout=10)
            if response.status_code == 404:
                logger.warning(f"Role {intern_role_id} not found in Zoho CRM")
                return False
            elif response.status_code == 403:
                logger.error(f"Access denied for role {intern_role_id} - check permissions")
                return False
            elif not response.ok:
                logger.error(f"API error {response.status_code} for role {intern_role_id}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating API access for role {intern_role_id}: {e}")
            return False

    def sync_role_deals(self, intern_role_id: str) -> int:
        """
        Sync deals for a specific intern role and return count of rejected/closed deals.
        """
        today = date.today()

        def update_sync(count: int) -> int:
            RoleDealSync.objects.update_or_create(
                intern_role_id=intern_role_id,
                defaults={
                    'total_rejected_deals': count,
                    'last_sync_date': today
                }
            )
            return count

        # Check cached sync
        try:
            existing_sync = RoleDealSync.objects.get(intern_role_id=intern_role_id)
            logger.debug(f"Using cached deal count for role {intern_role_id}: {existing_sync.total_rejected_deals}")
            return existing_sync.total_rejected_deals
        except RoleDealSync.DoesNotExist:
            pass

        # Get access token
        access_token = auth.get_access_token()
        if not access_token:
            logger.error(f"Could not get access token for role {intern_role_id}")
            return update_sync(0)

        # Validate API access
        if not self.validate_api_access(intern_role_id):
            logger.warning(f"Cannot access role {intern_role_id} - skipping deal sync")
            return update_sync(0)

        api_url = f"https://www.zohoapis.com/crm/v2/Intern_Roles/{intern_role_id}/Deals"
        headers = {'Authorization': f'Zoho-oauthtoken {access_token}'}
        params = {'fields': 'Deal_Name,Account_Name,Stage,Type'}

        try:
            time.sleep(0.05)  # Avoid hitting rate limits
            logger.debug(f"Requesting: {api_url}")
            response = requests.get(api_url, headers=headers, params=params, timeout=30)

            if response.status_code == 204:
                logger.info(f"No deals found for role {intern_role_id} (204)")
                return update_sync(0)

            response.raise_for_status()
            response_text = response.text.strip()
            if not response_text or response_text.lower().startswith('<!doctype'):
                logger.error(f"Invalid response (empty/HTML) for role {intern_role_id}")
                return update_sync(0)

            try:
                data = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON for role {intern_role_id}: {e}")
                logger.debug(f"Raw response (first 500 chars): {response_text[:500]}")
                return update_sync(0)

            if isinstance(data, dict) and data.get('error'):
                logger.error(f"API error for role {intern_role_id}: {data['error']}")
                return update_sync(0)

            deals = data.get('data', []) if isinstance(data, dict) else []
            logger.debug(f"Found {len(deals)} deals for role {intern_role_id}")

            rejected_closed_count = sum(
                1 for d in deals if isinstance(d, dict) and any(
                    kw in d.get('Stage', '').lower() for kw in ['rejected', 'closed']
                )
            )

            logger.info(
                f"Synced deals for role {intern_role_id}: "
                f"{rejected_closed_count} rejected/closed out of {len(deals)}"
            )
            return update_sync(rejected_closed_count)

        except requests.Timeout as e:
            logger.error(f"Timeout syncing deals for role {intern_role_id}: {e}")
            return update_sync(0)
        except requests.RequestException as e:
            logger.error(f"HTTP error for role {intern_role_id}: {e}")
            if getattr(e, 'response', None):
                logger.error(f"HTTP {e.response.status_code}: {e.response.text[:200]}")
            return update_sync(0)
        except Exception as e:
            logger.exception(f"Unexpected error syncing deals for role {intern_role_id}: {e}")
            return update_sync(0)

    
    
    def extract_json_field(self, field_value: str) -> List[str]:
        """
        Extract and parse JSON field values, handle various formats
        """
        if not field_value:
            return []
        
        try:
            # If it's already a list
            if isinstance(field_value, list):
                return [str(item).strip() for item in field_value if item]
            
            # If it's a string, try to parse as JSON
            if isinstance(field_value, str):
                # Clean the string
                cleaned = field_value.strip()
                
                # Try to parse as JSON
                try:
                    parsed = json.loads(cleaned)
                    if isinstance(parsed, list):
                        return [str(item).strip() for item in parsed if item]
                    elif isinstance(parsed, str):
                        return [parsed.strip()] if parsed.strip() else []
                except json.JSONDecodeError:
                    # If not JSON, split by common delimiters
                    if ',' in cleaned:
                        return [item.strip() for item in cleaned.split(',') if item.strip()]
                    elif ';' in cleaned:
                        return [item.strip() for item in cleaned.split(';') if item.strip()]
                    elif '|' in cleaned:
                        return [item.strip() for item in cleaned.split('|') if item.strip()]
                    else:
                        return [cleaned] if cleaned else []
            
            return []
            
        except (json.JSONDecodeError, AttributeError):
            logger.warning(f"Failed to parse field value: {field_value}")
            return []
    
    def get_contact_interests(self, contact: Contact) -> List[str]:
        """
        Extract all interests/industries from contact fields including the new ones
        """
        interests = []
        
        # Extract from various industry fields
        if contact.industry:
            interests.extend(self.extract_json_field(contact.industry))
        
        if contact.industry_choice_1:
            interests.extend(self.extract_json_field(contact.industry_choice_1))
        
        if contact.industry_choice_2:
            interests.extend(self.extract_json_field(contact.industry_choice_2))
        
        if contact.industry_choice_3:
            interests.extend(self.extract_json_field(contact.industry_choice_3))
        
        if contact.industry_1_areas:
            interests.extend(self.extract_json_field(contact.industry_1_areas))
        
        if contact.industry_2_areas:
            interests.extend(self.extract_json_field(contact.industry_2_areas))
        
        # Remove duplicates and normalize
        unique_interests = []
        for interest in interests:
            normalized = interest.lower().strip()
            if normalized and normalized not in [i.lower() for i in unique_interests]:
                unique_interests.append(interest.strip())
        
        return unique_interests
    
    def get_role_tags(self, role: InternRole) -> List[str]:
        """
        Extract role tags from intern role
        """
        if not role.role_tags:
            return []
        
        return self.extract_json_field(role.role_tags)
    
    def check_industry_match(self, contact_interests: List[str], role_tags: List[str]) -> Tuple[bool, List[str]]:
        """
        Check if contact interests match with role tags using ChatGPT with fallback
        Returns (is_match, matched_items)
        """
        # print(f"Checking industry match...", contact_interests, role_tags)
        if not contact_interests or not role_tags:
            return False, []
        
        # Try ChatGPT matching first if available
        if self.client:
            try:
                return self.check_industry_match_with_gpt(contact_interests, role_tags)
            except Exception as e:
                logger.warning(f"GPT matching failed, falling back to traditional matching: {e}")
        
        #Fallback to traditional matching
        return self.check_industry_match_traditional(contact_interests, role_tags)
    
    def check_industry_match_traditional(self, contact_interests: List[str], role_tags: List[str]) -> Tuple[bool, List[str]]:
        """
        Traditional industry matching using exact, partial, and fuzzy matching.
        Returns (is_match, matched_items)
        """
        if not contact_interests or not role_tags:
            return False, []
        
        matched_items = []
        contact_interests_lower = [interest.lower().strip() for interest in contact_interests]
        role_tags_lower = [tag.lower().strip() for tag in role_tags]

        # 1. Exact matches
        for tag, tag_lower in zip(role_tags, role_tags_lower):
            if tag_lower in contact_interests_lower:
                matched_items.append(f"{tag} (exact match)")

        # 2. Partial substring matches
        if not matched_items:
            for tag, tag_lower in zip(role_tags, role_tags_lower):
                for interest, interest_lower in zip(contact_interests, contact_interests_lower):
                    if (tag_lower in interest_lower or interest_lower in tag_lower) and len(tag_lower) > 2:
                        matched_items.append(f"{interest} ~ {tag}")
                        break

        # 3. Fuzzy similarity matches (handles typos / abbreviations)
        if not matched_items:
            threshold = 0.7  # similarity threshold (0–1)
            for tag, tag_lower in zip(role_tags, role_tags_lower):
                for interest, interest_lower in zip(contact_interests, contact_interests_lower):
                    similarity = SequenceMatcher(None, tag_lower, interest_lower).ratio()
                    if similarity >= threshold:
                        matched_items.append(f"{interest} ≈ {tag} ({similarity:.2f})")
                        break
        
        return len(matched_items) > 0, matched_items

    
    def check_industry_match_with_gpt(self, contact_interests: List[str], role_tags: List[str]) -> Tuple[bool, List[str]]:
        """
        Uses GPT to check if contact interests match role tags.
        Returns (is_match, matched_items)
        """
        if not contact_interests or not role_tags:
            return False, []
        # print("GPT matching for contact interests:", contact_interests, "and role tags:", role_tags)
        prompt = f"""
        You are a smart matcher. Given two lists:
        Interests: {contact_interests}
        Roles: {role_tags}

        Match items from Interests to Roles that mean the same thing,
        even if they are abbreviated, misspelled, or partially overlapping.

        Return ONLY a JSON array of string mappings (no markdown formatting), e.g.:
        ["Software Engineer -> Software Eng.", "Marketing -> Digital Marketing"]
        
        If no matches found, return: []
        """
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",  # you can also use "gpt-4o" or "gpt-3.5-turbo"
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        content = response.choices[0].message.content.strip()
        
        # Clean up GPT response - remove markdown code blocks if present
        if content.startswith("```"):
            # Remove code block markers
            lines = content.split('\n')
            # Find start and end of JSON content
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith('```'):
                    if not in_json:
                        in_json = True
                    else:
                        break
                elif in_json:
                    json_lines.append(line)
            content = '\n'.join(json_lines).strip()
        
        # Additional cleanup - remove 'json' if it appears at the start
        if content.lower().startswith('json'):
            content = content[4:].strip()
        
        # Try to parse GPT output
        matched_items = []
        try:
            # First, check if it's a malformed JSON array containing JSON parts
            if content.startswith("[") and content.endswith("]"):
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, list):
                        # Check if it looks like fragmented JSON parts
                        if any("```" in str(item) for item in parsed):
                            # Reconstruct the JSON from fragments
                            json_parts = []
                            for item in parsed:
                                item_str = str(item).strip()
                                if not item_str.startswith("```") and item_str not in ["[", "]", "json"]:
                                    json_parts.append(item_str)
                            
                            # Try to parse the reconstructed JSON
                            if json_parts:
                                reconstructed = "[" + ",".join(json_parts) + "]"
                                try:
                                    matched_items = json.loads(reconstructed)
                                    matched_items = [str(item).strip() for item in matched_items if item]
                                except:
                                    # Fallback: clean up individual items
                                    for part in json_parts:
                                        clean_part = part.strip().strip('"').strip(',')
                                        if clean_part and '->' in clean_part:
                                            matched_items.append(clean_part)
                        else:
                            # Normal JSON array
                            matched_items = [str(item).strip() for item in parsed if item]
                except json.JSONDecodeError:
                    # Continue with fallback parsing
                    pass
            
            # If no matches found yet, try direct JSON parsing
            if not matched_items and content.startswith("[") and content.endswith("]"):
                matched_items = json.loads(content)
                matched_items = [str(item).strip() for item in matched_items if item]
            
            # Final fallback: split by newlines and clean up
            if not matched_items:
                lines = [line.strip() for line in content.split('\n') if line.strip()]
                matched_items = [line for line in lines if '->' in line or line]
                
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse GPT JSON response: {e}")
            logger.debug(f"Raw GPT response: {content}")
            # Fallback: try to extract meaningful lines
            lines = content.split('\n')
            matched_items = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('```') and not line.lower() == 'json':
                    # Remove quotes if present
                    if line.startswith('"') and line.endswith('",'):
                        line = line[1:-2]
                    elif line.startswith('"') and line.endswith('"'):
                        line = line[1:-1]
                    if line:
                        matched_items.append(line)

        # Filter out empty or invalid matches
        valid_matches = []
        for item in matched_items:
            if item and isinstance(item, str):
                # Clean up the item
                clean_item = item.strip()
                # Remove trailing quotes and commas
                if clean_item.endswith('"'):
                    clean_item = clean_item[:-1]
                if clean_item.endswith(','):
                    clean_item = clean_item[:-1]
                # Skip empty items, brackets, and non-meaningful content
                if (clean_item and 
                    clean_item not in ['[', ']', '[]'] and 
                    not clean_item.startswith('```') and
                    clean_item.lower() != 'json'):
                    valid_matches.append(clean_item)
        
        # print("GPT matched items:", valid_matches)

        return len(valid_matches) > 0, valid_matches
        
    
    def check_location_match(self, contact: Contact, role: InternRole) -> bool:
        """
        Check if contact location matches role location
        """
        # Get contact location - try multiple fields
        contact_location = ""
        if contact.location:
            contact_location = contact.location
        elif contact.current_location_v2:
            contact_location = contact.current_location_v2
        
        contact_location = (contact_location or '').lower().strip()
        role_location = (role.location or '').lower().strip()
        
        if not contact_location or not role_location:
            return False
        
        # Exact match or contains match
        return (contact_location == role_location or 
                contact_location in role_location or 
                role_location in contact_location)
    
    def check_work_policy_match(self, contact: Contact, role: InternRole) -> bool:
        """
        Enhanced work policy compatibility check
        - Contact can have preferences for "work from office" and "hybrid"
        - If student is remote, match only with remote roles
        - If student prefers office/hybrid, match accordingly
        """
        # Get contact's work preferences - try multiple possible fields
        contact_location = (contact.location or '').lower()
        current_location = (contact.current_location_v2 or '').lower()
        
        # Check if contact is remote based on location
        is_contact_remote = ('remote' in contact_location or 
                           'remote' in current_location)
        
        # Check contact's work preferences (if available)
        contact_can_work_office = True  # Default assumption
        contact_can_work_hybrid = True  # Default assumption
        
        # Check role work policy from company
        role_policy = (role.company_work_policy or '').lower()
        role_remote = (role.open_to_remote or '').lower()
        
        # Determine role's work arrangements
        role_supports_remote = ('remote' in role_policy or 
                              'yes' in role_remote or 
                              'true' in role_remote)
        role_supports_office = ('office' in role_policy or 
                              'on-site' in role_policy or 
                              'onsite' in role_policy)
        role_supports_hybrid = ('hybrid' in role_policy or 
                              'flexible' in role_policy)
        
        # If no specific policy mentioned, assume hybrid/office
        if not any([role_supports_remote, role_supports_office, role_supports_hybrid]):
            role_supports_office = True
            role_supports_hybrid = True
        
        # Matching logic
        if is_contact_remote:
            # Remote contact needs remote-supporting role
            return role_supports_remote
        else:
            # Non-remote contact can match with office/hybrid roles
            # Contact supports both office and hybrid by default
            return (role_supports_office and contact_can_work_office) or \
                   (role_supports_hybrid and contact_can_work_hybrid) or \
                   role_supports_remote  # Remote is always acceptable
    
    def get_contact_skills(self, contact_id: str) -> List[str]:
        """
        Get skills for a contact from the Skills table
        """
        try:
            skills = Skill.objects.filter(contact_id=contact_id).values_list('skill_name', flat=True)
            return list(skills)
        except Exception as e:
            logger.error(f"Error fetching skills for contact {contact_id}: {e}")
            return []
    
    def check_skill_match(self, contact_id: str, role: "InternRole") -> Tuple[bool, List[str]]:
        """
        Enhanced skill matching using extracted skills from CVs.
        Matches exact words or relevant partials without accidental substring matches.
        Handles short skills like 'C', 'C++', 'R'.
        """
        contact_skills = self.get_contact_skills(contact_id)
        
        # Extract required skills from role description and function
        role_description = (role.role_description_requirements or '').lower()
        role_function = (role.role_function or '').lower()
        role_text = f"{role_description} {role_function}".strip()
        
        if not contact_skills or not role_text:
            return False, []
        
        matched_skills = []

        # Tokenize role text (safe word-level matching, includes short words)
        role_tokens = set(re.findall(r"[a-zA-Z0-9\+\#]+", role_text.lower()))

        for skill in contact_skills:
            skill_lower = skill.lower().strip()

            #  Special handling for very short skills (1–2 chars or symbols like "c++", "c#", "r")
            if len(skill_lower) <= 2 or any(ch in skill_lower for ch in ["+", "#"]):
                if skill_lower in role_tokens:
                    matched_skills.append(skill)
                continue
            
            #  Exact match (whole word, e.g. "git" not matching "digital")
            if re.search(rf"\b{re.escape(skill_lower)}\b", role_text):
                matched_skills.append(skill)
                continue

            #  Partial match (multi-word skills like "python programming" match "python")
            skill_tokens = skill_lower.split()
            if any(token in role_tokens for token in skill_tokens if len(token) > 3):
                matched_skills.append(skill)
        
        return len(matched_skills) > 0, matched_skills
    
    def check_company_dnc_status(self, role: InternRole) -> bool:
        """
        Check if the company associated with the role has is_dnc = True
        Returns True if company should be excluded (is_dnc = True)
        """
        if not role.intern_company_id:
            return False
        
        try:
            account = Account.objects.get(id=role.intern_company_id)
            return account.is_dnc
        except Account.DoesNotExist:
            logger.warning(f"Account {role.intern_company_id} not found for role {role.id}")
            return False
    
    def check_company_follow_up_date(self, role: InternRole) -> bool:
        """
        Check if company's follow_up_date is valid (future date from today)
        Returns True if company should be excluded (empty, today, or past date)
        """
        if not role.intern_company_id:
            return False

        try:
            account = Account.objects.get(id=role.intern_company_id)
            if not account.follow_up_date:
                return True  # Exclude if follow_up_date is empty
            
            
            follow_today = timezone.now().date()
            follow_up_date = account.follow_up_date.date() if hasattr(account.follow_up_date, 'date') else account.follow_up_date
            return follow_up_date <= follow_today 
            
        except Account.DoesNotExist:
            return False
    
    def check_intern_to_employee_ratio(self, role: InternRole, contact: Contact = None) -> bool:
        """
        Check if Intern-to-employee ratio ≤ 1:4
        Returns True if company should be excluded (ratio > 1:4)
        Now includes filter for contact start_date between deals start and end date
        """
        if not role.intern_company_id:
            return False
        
        try:
            account = Account.objects.get(id=role.intern_company_id)
            
            # Get employee count from account
            if not account.no_employees:
                return True  # Exclude if no employee count available
            
            try:
                employee_count = int(account.no_employees)
            except (ValueError, TypeError):
                return True  # Exclude if employee count is not a valid number
            
            # Count "Role Confirmed" deals for this company
            # Base query for confirmed deals
            confirmed_deals_query = Deal.objects.filter(
                account_id=role.intern_company_id,
                stage__icontains='Role Confirmed'
            )
            
            # If contact is provided and has start_date, filter deals by date range
            if contact and contact.start_date:
                try:
                    # Convert contact start date to date object if needed
                    contact_start_date = contact.start_date.date() if hasattr(contact.start_date, 'date') else contact.start_date
                    
                    # Filter deals where contact start_date is between deal start_date and end_date
                    confirmed_deals_query = confirmed_deals_query.filter(
                        start_date__lte=contact_start_date,
                        end_date__gte=contact_start_date
                    ).exclude(
                        start_date__isnull=True
                    ).exclude(
                        end_date__isnull=True
                    )
                    
                    logger.info(f"Filtered confirmed deals for contact start date {contact_start_date}")
                    
                except Exception as e:
                    logger.warning(f"Error filtering deals by contact start date: {e}")
                    # Continue with original query if date filtering fails
            
            confirmed_deals_count = confirmed_deals_query.count()
            
            # If no confirmed deals, ratio is good
            if confirmed_deals_count == 0:
                return False
            
            # Calculate ratio: confirmed_deals : employee_count
            # We want ratio ≤ 1:4, which means confirmed_deals/employee_count ≤ 1/4
            ratio = confirmed_deals_count / employee_count
            
            # Exclude if ratio > 1:4 (0.25)
            return ratio > 0.25
            
        except Account.DoesNotExist:
            logger.warning(f"Account {role.intern_company_id} not found for role {role.id}")
            return False
        except Exception as e:
            logger.error(f"Error checking intern-to-employee ratio for role {role.id}: {e}")
            return False
    
    def check_active_deals_limit(self, role: InternRole) -> bool:
        """
        Check if company has more than 3 active deals in Scheduling/Interviewing/Pending Outcome stages
        Returns True if company should be excluded (more than 3 active deals)
        """
        if not role.intern_company_id:
            return False
        
        try:
            # Count active deals in the specified stages
            active_deals_count = Deal.objects.filter(
                account_id=role.intern_company_id,
                stage__in=['Scheduling Interview', 'Pending Interview','Rescheduling Interview', 'Pending Outcome']
            ).count()
            
            # Also check for case-insensitive partial matches
            if active_deals_count == 0:
                active_deals_count = Deal.objects.filter(
                    Q(account_id=role.intern_company_id) &
                    (Q(stage__icontains='Scheduling Interview') | 
                     Q(stage__icontains='Pending Interview') | 
                     Q(stage__icontains='Rescheduling Interview') |
                     Q(stage__icontains='Pending Outcome'))
                ).count()
            
            # Exclude if more than 3 active deals
            return active_deals_count > 3
            
        except Exception as e:
            logger.error(f"Error checking active deals limit for role {role.id}: {e}")
            return False
    
    def check_start_date_priority(self, contact: Contact, role: InternRole) -> bool:
        """
        Check if candidate's start date falls within 2 weeks before any confirmed role's end date for the company
        Returns True if the candidate should be prioritized (start date within 2 weeks of any confirmed role end date)
        """
        if not contact.start_date or not role.intern_company_id:
            return False
        
        try:
            # Convert contact start date to date object if needed
            contact_start_date = contact.start_date.date() if hasattr(contact.start_date, 'date') else contact.start_date
            
            # Get all confirmed deals for this company
            confirmed_deals = Deal.objects.filter(
                account_id=role.intern_company_id,
                stage__icontains='Role Confirmed'
            )
            
            # Check if contact's start date is within 2 weeks of any confirmed role's end date
            from datetime import timedelta
            
            for deal in confirmed_deals:
                if deal.end_date:
                    # Convert deal end date to date object if needed
                    deal_end_date = deal.end_date.date() if hasattr(deal.end_date, 'date') else deal.end_date
                    
                    # Calculate the date 2 weeks (14 days) before the deal end date
                    two_weeks_before_end = deal_end_date - timedelta(days=14)
                    
                    # Check if contact's start date is within 2 weeks before the deal end date
                    # and not after the deal end date
                    if two_weeks_before_end <= contact_start_date <= deal_end_date:
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking start date priority for contact {contact.id} and role {role.id}: {e}")
            return False
    
    def calculate_match_score(self, industry_1_match: bool, industry_2_match: bool,
                          skill_match: bool, matched_industry_1: List[str],
                          matched_industry_2: List[str], matched_skills: List[str],
                          start_date_priority: bool = False,
                          rejected_deals_count: int = 0) -> float:
        """
        Calculate overall match score based on new industry weights and skill match.
        """
        score = 0.0

        if industry_1_match:
            score += 0.40 * min(1.0, len(matched_industry_1) / 1.0)
            i_score = 0.40 * min(1.0, len(matched_industry_1) / 1.0)
            logger.info(f"Industry match score: {i_score:.2f} with {len(matched_industry_1)} matched industries")
        if industry_2_match:
            score += 0.20 * min(1.0, len(matched_industry_2) / 1.0)
            i2_score = 0.20 * min(1.0, len(matched_industry_2) / 1.0)
            logger.info(f"Industry 2 match score: {i2_score:.2f} with {len(matched_industry_2)} matched industries")
        if skill_match:
            score += 0.25 * min(1.0, len(matched_skills) / 3.0)
            s_score = 0.25 * min(1.0, len(matched_skills) / 3.0)
            logger.info(f"Skill match score: {s_score:.2f} with {len(matched_skills)} matched skills")
        if start_date_priority:
            score += 0.15
            logger.info(f"Start date priority bonus applied: 0.15")
        if rejected_deals_count >= 2:
            score -= 0.15
            logger.info(f"Rejected deals penalty applied: {rejected_deals_count} rejected deals (-0.15)")

        return max(0.0, min(1.0, score))

    
    
    def get_contact_industries(self, contact: Contact) -> Tuple[List[str], List[str]]:
        """
        Extract industry_1 and industry_2 interests from contact fields.
        """
        industry_1 = []
        industry_2 = []

        if contact.industry_choice_1:
            industry_1.append(contact.industry_choice_1)
        if contact.industry_1_areas:
            industry_1.extend(self.extract_json_field(contact.industry_1_areas))

        if contact.industry_choice_2:
            industry_2.append(contact.industry_choice_2)
        if contact.industry_2_areas:
            industry_2.extend(self.extract_json_field(contact.industry_2_areas))

        # Normalize and deduplicate
        industry_1 = list({i.strip().lower() for i in industry_1 if i})
        industry_2 = list({i.strip().lower() for i in industry_2 if i})

        return industry_1, industry_2
    
    def find_matches_for_contact(self, contact_id: str) -> List[Dict[str, Any]]:
        """
        Find all potential matches for a given contact with new industry logic.
        """
        try:
            contact = Contact.objects.get(id=contact_id)
        except Contact.DoesNotExist:
            return []

        industry_1, industry_2 = self.get_contact_industries(contact)
        matches = []

        roles = InternRole.objects.filter(Q(company_work_policy__icontains='Hybrid') | Q(company_work_policy__icontains='Office-based')) 
        for role in roles:
            try:
                rejected_deals_count = self.sync_role_deals(role.id)

                if self.check_company_dnc_status(role):
                    continue
                if self.check_intern_to_employee_ratio(role, contact):
                    continue
                if self.check_active_deals_limit(role):
                    continue
                if self.check_location_match(contact, role) is False:
                    continue

                role_tags = self.get_role_tags(role)
                # Check industry_1 match
                industry_1_match, matched_industry_1 = self.check_industry_match(industry_1, role_tags)
                # Check industry_2 match
                industry_2_match, matched_industry_2 = self.check_industry_match(industry_2, role_tags)

                # Skip job if no industry match
                if not industry_1_match and not industry_2_match:
                    continue

                skill_match, matched_skills = self.check_skill_match(contact_id, role)
                start_date_priority = self.check_start_date_priority(contact, role)

                match_score = self.calculate_match_score(
                    industry_1_match, industry_2_match, skill_match,
                    matched_industry_1, matched_industry_2, matched_skills,
                    start_date_priority, rejected_deals_count
                )

                if match_score > 0.1:
                    match_reason_parts = []
                    if industry_1_match:
                        match_reason_parts.append(f"Industry 1: {', '.join(matched_industry_1[:3])}")
                    if industry_2_match:
                        match_reason_parts.append(f"Industry 2: {', '.join(matched_industry_2[:3])}")
                    if skill_match:
                        match_reason_parts.append(f"Skills: {', '.join(matched_skills[:3])}")
                    if start_date_priority:
                        match_reason_parts.append("Start date priority (+0.10)")
                    if rejected_deals_count >= 2:
                        match_reason_parts.append(f"Rejected deals penalty ({rejected_deals_count} deals, -0.10)")

                    matches.append({
                        'contact_id': contact_id,
                        'intern_role_id': role.id,
                        'match_score': match_score,
                        'industry_1_match': industry_1_match,
                        'industry_2_match': industry_2_match,
                        'skill_match': skill_match,
                        'start_date_priority': start_date_priority,
                        'matched_industry_1': matched_industry_1,
                        'matched_industry_2': matched_industry_2,
                        'matched_skills': matched_skills,
                        'match_reason': '; '.join(match_reason_parts),
                        'role_title': role.role_title or role.name,
                        'company_name': role.intern_company_name,
                    })

            except Exception as e:
                logger.error(f"Error matching contact {contact_id} with role {role.id}: {e}")
                continue

        matches.sort(key=lambda x: x['match_score'], reverse=True)

        # Send email if no matches found
        if not matches:
            self.send_no_match_email(contact)

        return matches

    
    def send_no_match_email(self, contact: Contact):
        """
        Send styled HTML email to molly@beyondacademy.com if no matches found for contact.
        """
        logger.info(f"Sending no-match email for contact {contact.id}")

        subject = f"No Job Matches Found for {contact.full_name}"

        plain_message = (
            f"Hello Molly,\n\n"
            f"No job matches were found for candidate {contact.full_name} (ID: {contact.id}).\n\n"
            f"Best regards,\nBeyond Academy System"
        )

        html_message = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333;">
            <h2 style="color: #d9534f;">⚠ No Job Matches Found</h2>
            <p>Hello Molly,</p>
            <p>
                Unfortunately, we could not find any job matches for 
                <strong>{contact.full_name}</strong> (ID: <strong>{contact.id}</strong>).
            </p>
            <p style="margin-top: 20px;">
                <em>Next Steps:</em>
            </p>
            <ul>
                <li>Review the candidate’s profile.</li>
                <li>Check if additional opportunities are available.</li>
                <li>Consider updating the job search criteria.</li>
            </ul>
            <p style="margin-top: 20px;">
                Best regards,<br>
                <strong>Beyond Academy System</strong>
            </p>
        </body>
        </html>
        """

        send_mail(
            subject,
            plain_message,  # fallback for clients that don’t support HTML
            settings.EMAIL_HOST_USER,
            ['molly@beyondacademy.com', 'prabhat.scaleupally@gmail.com'],
            html_message=html_message
        )   
    def store_matches(self, matches: List[Dict[str, Any]]) -> int:
        """
        Store job matches in the database
        """
        stored_count = 0
        
        for match_data in matches:
            try:
                with transaction.atomic():
                    # Check if match already exists
                    existing_match = JobMatch.objects.filter(
                        contact_id=match_data['contact_id'],
                        intern_role_id=match_data['intern_role_id']
                    ).first()
                    
                    if existing_match:
                        # Update existing match
                        existing_match.match_score = match_data.get('match_score', 0.0)
                        existing_match.industry_match = match_data.get('industry_match', False)
                        existing_match.location_match = match_data.get('location_match', False)
                        existing_match.work_policy_match = match_data.get('work_policy_match', False)
                        existing_match.skill_match = match_data.get('skill_match', False)
                        existing_match.matched_skills = json.dumps(match_data.get('matched_skills', []))
                        existing_match.match_reason = match_data.get('match_reason', '')
                        existing_match.industry_1_match = match_data.get('industry_1_match', False)
                        existing_match.industry_2_match = match_data.get('industry_2_match', False)
                        existing_match.matched_industry_1 = json.dumps(match_data.get('matched_industry_1', []))
                        existing_match.matched_industry_2 = json.dumps(match_data.get('matched_industry_2', []))
                        existing_match.status = 'active'
                        existing_match.save()
                    else:
                        # Create new match
                        JobMatch.objects.create(
                            contact_id=match_data.get('contact_id'),
                            intern_role_id=match_data.get('intern_role_id'),
                            match_score=match_data.get('match_score', 0.0),
                            industry_match=match_data.get('industry_match', False),
                            location_match=match_data.get('location_match', False),
                            work_policy_match=match_data.get('work_policy_match', False),
                            skill_match=match_data.get('skill_match', False),
                            matched_skills=json.dumps(match_data.get('matched_skills', [])),
                            match_reason=match_data.get('match_reason', ''),
                            industry_1_match=match_data.get('industry_1_match', False),
                            industry_2_match=match_data.get('industry_2_match', False),
                            matched_industry_1=json.dumps(match_data.get('matched_industry_1', [])),
                            matched_industry_2=json.dumps(match_data.get('matched_industry_2', [])),
                            status='active'
                        )
                    
                    stored_count += 1
                    
            except Exception as e:
                logger.error(f"Error storing match: {e}")
                continue
        
        return stored_count
    
    def process_contact_matches(self, contact_id: str) -> Dict[str, Any]:
        """
        Process job matches for a single contact
        """
        logger.info(f"Processing matches for contact {contact_id}")
        
        try:
            # Find matches
            matches = self.find_matches_for_contact(contact_id)
            
            # Store matches
            stored_count = self.store_matches(matches)
            
            return {
                'contact_id': contact_id,
                'total_matches': len(matches),
                'stored_matches': stored_count,
                'top_match_score': matches[0]['match_score'] if matches else 0.0,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error processing matches for contact {contact_id}: {e}")
            return {
                'contact_id': contact_id,
                'total_matches': 0,
                'stored_matches': 0,
                'top_match_score': 0.0,
                'status': 'error',
                'error': str(e)
            }
    
    def process_all_contacts(self) -> Dict[str, int]:
        """
        Process job matches for all contacts
        """
        logger.info("Processing job matches for all contacts")
        
        # Get all contacts
        contacts = Contact.objects.all()
        
        total_contacts = contacts.count()
        contacts_with_matches = 0
        total_matches = 0
        
        logger.info(f"Processing {total_contacts} contacts...")
        
        for i, contact in enumerate(contacts, 1):
            try:
                result = self.process_contact_matches(contact.id)
                
                if result['total_matches'] > 0:
                    contacts_with_matches += 1
                    total_matches += result['stored_matches']
                
                # Log progress
                if i % 50 == 0:
                    logger.info(f"Processed {i}/{total_contacts} contacts...")
                    
            except Exception as e:
                logger.error(f"Error processing contact {contact.id}: {e}")
                continue
        
        logger.info(f"Job matching completed: {contacts_with_matches}/{total_contacts} contacts have matches")
        
        return {
            'total_contacts': total_contacts,
            'contacts_with_matches': contacts_with_matches,
            'total_matches': total_matches
        }
    
    def get_matches_for_contact(self, contact_id: str, limit: int = 10) -> List[JobMatch]:
        """
        Get stored matches for a contact
        """
        try:
            matches = JobMatch.objects.filter(
                contact_id=contact_id,
                status='active'
            ).order_by('-match_score')[:limit]
            
            return list(matches)
            
        except Exception as e:
            logger.error(f"Error fetching matches for contact {contact_id}: {e}")
            return []


def match_jobs_for_contact(contact_id: str, min_score: float = 0.2) -> Dict[str, Any]:
    """
    Enhanced standalone function to match jobs for a specific contact
    This function can be called from webhook handlers or other modules
    
    Args:
        contact_id: Zoho contact ID
        min_score: Minimum match score threshold (default 0.2)
        
    Returns:
        Dictionary with matching results
    """
    logger.info(f"Starting enhanced job matching for contact {contact_id}")
    
    matcher = JobMatcher()
    try:
        # Get all potential matches
        matches = matcher.find_matches_for_contact(contact_id)
        
        if not matches:
            logger.info(f"No matches found for contact {contact_id}")
            return {
                'contact_id': contact_id,
                'total_matches': 0,
                'matches_created': 0,
                'matches_updated': 0,
                'message': 'No suitable job matches found'
            }
        
        # Filter matches by minimum score
        quality_matches = [match for match in matches if match['match_score'] >= min_score]
        
        matches_created = 0
        matches_updated = 0
        
        with transaction.atomic():
            # Remove existing matches for this contact
            JobMatch.objects.filter(contact_id=contact_id).delete()
            logger.info(f"Removed existing matches for contact {contact_id}")
            
            # Create new matches
            for match in quality_matches:
                try:
                    job_match = JobMatch.objects.create(
                        contact_id=contact_id,
                        intern_role_id=match['intern_role_id'],
                        match_score=match['match_score'],
                        industry_1_match=match['industry_1_match'],
                        industry_2_match=match['industry_2_match'],
                        skill_match=match['skill_match'],
                        matched_industry_1=json.dumps(match['matched_industry_1']),
                        matched_industry_2=json.dumps(match['matched_industry_2']),
                        matched_skills=json.dumps(match['matched_skills']),
                        match_reason=match['match_reason'],
                        status='active'
                    )
                    matches_created += 1
                    logger.debug(f"Created match: {contact_id} -> {match['intern_role_id']} (score: {match['match_score']:.2f})")
                    
                except Exception as e:
                    logger.error(f"Error creating match for role {match['intern_role_id']}: {e}")
                    continue
        
        result = {
            'contact_id': contact_id,
            'total_matches': len(matches),
            'quality_matches': len(quality_matches),
            'matches_created': matches_created,
            'matches_updated': matches_updated,
            'min_score_threshold': min_score,
            'top_matches': quality_matches[:5],  # Top 5 matches for reference
            'message': f'Successfully processed {matches_created} job matches',
            'status': 'success'
        }
        
        logger.info(f"Enhanced job matching completed for contact {contact_id}: {matches_created} quality matches created")
        return result
        
    except Contact.DoesNotExist:
        error_msg = f"Contact {contact_id} not found in database"
        logger.error(error_msg)
        return {
            'contact_id': contact_id,
            'error': error_msg,
            'total_matches': 0,
            'matches_created': 0,
            'status': 'error'
        }
    except Exception as e:
        error_msg = f"Error in enhanced job matching for contact {contact_id}: {str(e)}"
        logger.error(error_msg)
        return {
            'contact_id': contact_id,
            'error': error_msg,
            'total_matches': 0,
            'matches_created': 0,
            'status': 'error'
        }


def batch_match_jobs_for_contacts(contact_ids: List[str], min_score: float = 0.2) -> Dict[str, Any]:
    """
    Batch job matching for multiple contacts
    
    Args:
        contact_ids: List of contact IDs to match
        min_score: Minimum match score threshold
        
    Returns:
        Summary of batch matching results
    """
    logger.info(f"Starting batch job matching for {len(contact_ids)} contacts")
    
    results = {
        'total_contacts': len(contact_ids),
        'successful_matches': 0,
        'failed_matches': 0,
        'total_matches_created': 0,
        'contact_results': {},
        'errors': []
    }
    
    for contact_id in contact_ids:
        try:
            result = match_jobs_for_contact(contact_id, min_score)
            
            if 'error' in result:
                results['failed_matches'] += 1
                results['errors'].append(f"Contact {contact_id}: {result['error']}")
            else:
                results['successful_matches'] += 1
                results['total_matches_created'] += result.get('matches_created', 0)
            
            results['contact_results'][contact_id] = result
            
        except Exception as e:
            results['failed_matches'] += 1
            error_msg = f"Contact {contact_id}: {str(e)}"
            results['errors'].append(error_msg)
            logger.error(f"Batch matching error for contact {contact_id}: {e}")
    
    logger.info(f"Batch job matching completed: {results['successful_matches']}/{results['total_contacts']} successful")
    return results


def main():
    """
    Main function to run job matching
    """
    matcher = JobMatcher()
    
    try:
        results = matcher.process_all_contacts()
        print(f" Job matching completed successfully!")
        print(f"Total contacts: {results['total_contacts']}")
        print(f"Contacts with matches: {results['contacts_with_matches']}")
        print(f"Total matches created: {results['total_matches']}")
        
    except Exception as e:
        logger.error(f"Job matching failed: {e}")
        print(f"❌ Job matching failed: {e}")


if __name__ == "__main__":
    main()
