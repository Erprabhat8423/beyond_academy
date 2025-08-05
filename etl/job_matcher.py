"""
Job Matching Service

This module implements the job matching logic between contacts (students) and intern roles.
Matching criteria:
- Role Tags = Primary/Secondary interest (industry fields)
- Student Location = Job Location
- Work Policy = Hybrid/In-office (unless student is remote, then remote only)
- Skills matching (optional enhancement)
"""
import json
import logging
from typing import List, Dict, Any, Tuple
from datetime import datetime
from django.db import transaction
from django.db.models import Q

from zoho_app.models import Contact, InternRole, JobMatch, Skill

# Set up logging
logger = logging.getLogger(__name__)


class JobMatcher:
    """
    Job matching service for matching contacts with intern roles
    """
    
    def __init__(self):
        pass
    
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
        Check if contact interests match with role tags
        Returns (is_match, matched_items)
        """
        if not contact_interests or not role_tags:
            return False, []
        
        matched_items = []
        contact_interests_lower = [interest.lower().strip() for interest in contact_interests]
        role_tags_lower = [tag.lower().strip() for tag in role_tags]
        
        # First check for exact matches
        for tag in role_tags:
            tag_lower = tag.lower().strip()
            if tag_lower in contact_interests_lower:
                matched_items.append(tag)
        
        # If no exact matches, check for partial matches
        if not matched_items:
            for tag in role_tags:
                tag_lower = tag.lower().strip()
                for interest in contact_interests:
                    interest_lower = interest.lower().strip()
                    if (tag_lower in interest_lower or interest_lower in tag_lower) and len(tag_lower) > 2:
                        matched_items.append(f"{interest} ~ {tag}")
                        break
        
        return len(matched_items) > 0, matched_items
    
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
    
    def check_skill_match(self, contact_id: str, role: InternRole) -> Tuple[bool, List[str]]:
        """
        Enhanced skill matching using extracted skills from CVs
        """
        contact_skills = self.get_contact_skills(contact_id)
        
        # Extract required skills from role description and function
        role_description = (role.role_description_requirements or '').lower()
        role_function = (role.role_function or '').lower()
        role_text = f"{role_description} {role_function}".strip()
        
        if not contact_skills or not role_text:
            return False, []
        
        matched_skills = []
        
        # Direct skill matching
        for skill in contact_skills:
            skill_lower = skill.lower().strip()
            if len(skill_lower) > 2:  # Skip very short skills
                # Check for exact mentions in role requirements
                if skill_lower in role_text:
                    matched_skills.append(skill)
                # Check for partial matching (for skills like "Python" matching "Python programming")
                elif any(word in role_text for word in skill_lower.split() if len(word) > 3):
                    matched_skills.append(skill)
        
        return len(matched_skills) > 0, matched_skills
    
    def calculate_match_score(self, industry_match: bool, location_match: bool, 
                            work_policy_match: bool, skill_match: bool,
                            matched_industries: List[str], matched_skills: List[str]) -> float:
        """
        Calculate overall match score based on different criteria
        """
        score = 0.0
        
        # Industry match is most important (40% weight)
        if industry_match:
            score += 0.4 * min(1.0, len(matched_industries) / 3.0)
        
        # Location match (25% weight)
        if location_match:
            score += 0.15
        
        # Work policy match (20% weight)
        if work_policy_match:
            score += 0.20
        
        # Skill match (15% weight)
        if skill_match:
            score += 0.25* min(1.0, len(matched_skills) / 5.0)
        
        return min(1.0, score)  # Cap at 1.0
    
    def find_matches_for_contact(self, contact_id: str) -> List[Dict[str, Any]]:
        """
        Find all potential matches for a given contact
        """
        try:
            contact = Contact.objects.get(id=contact_id)
        except Contact.DoesNotExist:
            logger.error(f"Contact {contact_id} not found")
            return []
        
        # Get contact interests
        contact_interests = self.get_contact_interests(contact)
        
        if not contact_interests:
            logger.info(f"No interests found for contact {contact_id}")
        
        matches = []
        
        # Get all active intern roles
        roles = InternRole.objects.filter(
            Q(role_status__isnull=True) | 
            ~Q(role_status__icontains='closed') &
            ~Q(role_status__icontains='cancelled')
        )
        
        for role in roles:
            try:
                # Check industry match
                role_tags = self.get_role_tags(role)
                industry_match, matched_industries = self.check_industry_match(contact_interests, role_tags)
                
                # Check location match
                location_match = self.check_location_match(contact, role)
                
                # Check work policy match
                work_policy_match = self.check_work_policy_match(contact, role)
                
                # Check skill match
                skill_match, matched_skills = self.check_skill_match(contact_id, role)
                
                # Calculate overall score
                match_score = self.calculate_match_score(
                    industry_match, location_match, work_policy_match, skill_match,
                    matched_industries, matched_skills
                )
                
                # Only include matches with some score
                if match_score > 0.1:  # 10% minimum threshold
                    match_reason_parts = []
                    if industry_match:
                        match_reason_parts.append(f"Industry: {', '.join(matched_industries[:3])}")
                    if location_match:
                        match_reason_parts.append("Location compatible")
                    if work_policy_match:
                        match_reason_parts.append("Work policy compatible")
                    if skill_match:
                        match_reason_parts.append(f"Skills: {', '.join(matched_skills[:3])}")
                    
                    matches.append({
                        'contact_id': contact_id,
                        'intern_role_id': role.id,
                        'match_score': match_score,
                        'industry_match': industry_match,
                        'location_match': location_match,
                        'work_policy_match': work_policy_match,
                        'skill_match': skill_match,
                        'matched_industries': matched_industries,
                        'matched_skills': matched_skills,
                        'match_reason': '; '.join(match_reason_parts),
                        'role_title': role.role_title or role.name,
                        'company_name': role.intern_company_name,
                    })
                    
            except Exception as e:
                logger.error(f"Error matching contact {contact_id} with role {role.id}: {e}")
                continue
        
        # Sort matches by score descending
        matches.sort(key=lambda x: x['match_score'], reverse=True)
        
        logger.info(f"Found {len(matches)} matches for contact {contact_id}")
        return matches
    
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
                        existing_match.match_score = match_data['match_score']
                        existing_match.industry_match = match_data['industry_match']
                        existing_match.location_match = match_data['location_match']
                        existing_match.work_policy_match = match_data['work_policy_match']
                        existing_match.skill_match = match_data['skill_match']
                        existing_match.matched_industries = json.dumps(match_data['matched_industries'])
                        existing_match.matched_skills = json.dumps(match_data['matched_skills'])
                        existing_match.match_reason = match_data['match_reason']
                        existing_match.status = 'active'
                        existing_match.save()
                    else:
                        # Create new match
                        JobMatch.objects.create(
                            contact_id=match_data['contact_id'],
                            intern_role_id=match_data['intern_role_id'],
                            match_score=match_data['match_score'],
                            industry_match=match_data['industry_match'],
                            location_match=match_data['location_match'],
                            work_policy_match=match_data['work_policy_match'],
                            skill_match=match_data['skill_match'],
                            matched_industries=json.dumps(match_data['matched_industries']),
                            matched_skills=json.dumps(match_data['matched_skills']),
                            match_reason=match_data['match_reason'],
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
                        industry_match=match['industry_match'],
                        location_match=match['location_match'],
                        work_policy_match=match['work_policy_match'],
                        skill_match=match['skill_match'],
                        matched_industries=json.dumps(match['matched_industries']),
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
        print(f"✅ Job matching completed successfully!")
        print(f"Total contacts: {results['total_contacts']}")
        print(f"Contacts with matches: {results['contacts_with_matches']}")
        print(f"Total matches created: {results['total_matches']}")
        
    except Exception as e:
        logger.error(f"Job matching failed: {e}")
        print(f"❌ Job matching failed: {e}")


if __name__ == "__main__":
    main()
