import json
import logging
import hmac
import hashlib
import threading
import asyncio
import os
import requests
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django.conf import settings
from django.utils.decorators import method_decorator
from django.views import View
from django.db import transaction
from django.utils import timezone

from .models import Contact, JobMatch, Skill, Document
from zoho.attachments import ZohoAttachmentManager
from zoho.api_client import ZohoClient
from etl.job_matcher import match_jobs_for_contact
from etl.pipeline import sync_contacts, sync_accounts, sync_intern_roles

logger = logging.getLogger(__name__)


class ZohoWebhookHandler:
    """Handles Zoho CRM webhook notifications"""
    
    def __init__(self):
        """Initialize the webhook handler"""
        # Get configuration from Django settings
        self.attachment_manager = ZohoAttachmentManager(
            download_dir=getattr(settings, 'CV_DOWNLOAD_DIR', 'downloads')
        )
        self.webhook_secret = getattr(settings, 'WEBHOOK_SECRET', 'your_webhook_secret_key_here')
        self.zoho_client = ZohoClient()
        
    def verify_webhook_signature(self, payload: str, signature: str) -> bool:
        """
        Verify the webhook signature from Zoho
        
        Args:
            payload: Raw webhook payload
            signature: Signature from Zoho webhook headers
            
        Returns:
            True if signature is valid
        """
        try:
            expected_signature = hmac.new(
                self.webhook_secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            return hmac.compare_digest(signature, expected_signature)
        except Exception as e:
            logger.error(f"Error verifying webhook signature: {e}")
            return False
    
    def process_contact_update(self, webhook_data: dict) -> dict:
        """
        Process contact update webhook notification with comprehensive data sync
        
        Args:
            webhook_data: Webhook payload data
            
        Returns:
            Processing result dictionary
        """
        try:
            contact_info = self.extract_contact_info(webhook_data)
            if not contact_info:
                return {'status': 'error', 'message': 'No contact information found in webhook'}
            
            contact_id = contact_info.get('id')
            if not contact_id:
                return {'status': 'error', 'message': 'No contact ID found'}
            
            # Step 1: Always fetch latest data from Zoho API to ensure full sync
            logger.info(f"Step 5. *********Fetching latest contact data from Zoho API for {contact_id}*********")
            full_contact_data = self.fetch_contact_from_api(contact_id)
            
            if full_contact_data:
                # Use API data as the primary source (more complete and up-to-date)
                contact_info = full_contact_data
                # logger.info(f"Using complete contact data from Zoho API for {contact_id}")
            else:
                # Fallback to webhook data if API fetch fails
                logger.warning(f"Could not fetch from API, using webhook data for contact {contact_id}")
            
            # Check if this is a "Ready to Pitch" contact for CV processing
            role_success_stage = contact_info.get('Role_Success_Stage', '').strip()
            if not role_success_stage:
                # Try alternate field names from webhook
                role_success_stage = contact_info.get('role_success_stage', '').strip()
            
            logger.info(f"Step 14. *********Contact {contact_id} role_success_stage: '{role_success_stage}' *********")
            
            # If not "Ready to Pitch", just return success for data update
            if role_success_stage != 'Ready to Pitch':
                return {
                    'status': 'success',
                    'contact_id': contact_id,
                    'message': f'Contact data updated. Role stage "{role_success_stage}" - CV processing skipped'
                }
            
            logger.info(f"Step 15. *********Processing Ready to Pitch contact: {contact_id}*********")
            
            # Start asynchronous processing for CV download and skill extraction
            self.start_async_processing(contact_id, contact_info)
            
            return {
                'status': 'success',
                'contact_id': contact_id,
                'message': f'Contact {contact_id} processing started (async)',
                'note': 'CV download, skill extraction, and job matching will be processed in background'
            }
                
        except Exception as e:
            logger.error(f"Error processing contact update: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def process_account_update(self, webhook_data: dict) -> dict:
        """
        Process account update webhook notification with comprehensive data sync
        
        Args:
            webhook_data: Webhook payload data
            
        Returns:
            Processing result dictionary
        """
        try:
            account_id = webhook_data.get('id')
            account_name = webhook_data.get('name', 'Unknown')
            
            # Step 1: Fetch complete account data from Zoho API
            logger.info(f"Step 6. *********Fetching latest account data from Zoho API for {account_id} *********")
            full_account_data = self.fetch_account_from_api(account_id)
            
            if full_account_data:
                # Use API data as the primary source (more complete and up-to-date)
                account_info = full_account_data
                logger.info(f"Using complete account data from Zoho API for {account_id}")
            else:
                # Fallback to webhook data if API fetch fails
                logger.warning(f"Could not fetch from API, using webhook data for account {account_id}")
                account_info = webhook_data
            
            # Step 2: Update local account data with the most current information
            logger.info(f"Step 7. *********Updating local account data for {account_id} *********")
            update_success = self.update_local_account(account_info)
            
            # Step 3: Sync deals associated with this account
            logger.info(f"Step 8. *********Syncing deals for account {account_id} *********")
            deals_synced = self.sync_account_deals(account_id)
            
            if update_success:
                logger.info(f"Step 9. *********Successfully updated local account data for {account_id} *********")
                return {
                    'status': 'success',
                    'account_id': account_id,
                    'account_name': account_name,
                    'deals_synced': deals_synced,
                    'message': f'Account {account_id} ({account_name}) data and {deals_synced} deals successfully updated'
                }
            else:
                return {
                    'status': 'error',
                    'account_id': account_id,
                    'deals_synced': deals_synced,
                    'message': f'Failed to update local account data for {account_id}, but synced {deals_synced} deals'
                }
                
        except Exception as e:
            logger.error(f"Error processing account update: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def process_intern_role_update(self, webhook_data: dict) -> dict:
        """
        Process intern role update webhook notification with comprehensive data sync
        
        Args:
            webhook_data: Webhook payload data
            
        Returns:
            Processing result dictionary
        """
        try:
            intern_role_id = webhook_data.get('id')
            intern_role_name = webhook_data.get('name') or webhook_data.get('Role_Title', 'Unknown')
            
            if not intern_role_id:
                return {'status': 'error', 'message': 'No intern role ID found in webhook data'}
            
            logger.info(f"Step 5. *********Processing intern role update for ID: {intern_role_id}, Name: {intern_role_name} *********")
            
            # Step 1: Fetch complete intern role data from Zoho API
            logger.info(f"Step 6. *********Fetching latest intern role data from Zoho API for {intern_role_id} *********")
            full_role_data = self.fetch_intern_role_from_api(intern_role_id)
            
            if full_role_data:
                # Use API data as the primary source (more complete and up-to-date)
                role_info = full_role_data
                logger.info(f"Using complete intern role data from Zoho API for {intern_role_id}")
            else:
                # Fallback to webhook data if API fetch fails
                logger.warning(f"Could not fetch from API, using webhook data for intern role {intern_role_id}")
                role_info = webhook_data
            
            # Step 2: Update local intern role data with the most current information
            logger.info(f"Step 7. *********Updating local intern role data for {intern_role_id} *********")
            update_success = self.update_local_intern_role(role_info)
            
            # Step 3: Sync role deals associated with this intern role
            logger.info(f"Step 8. *********Syncing role deals for intern role {intern_role_id} *********")
            deals_synced = self.sync_intern_role_deals(intern_role_id)
            
            if update_success:
                logger.info(f"Step 9. *********Successfully updated local intern role data for {intern_role_id} *********")
                return {
                    'status': 'success',
                    'intern_role_id': intern_role_id,
                    'intern_role_name': intern_role_name,
                    'deals_synced': deals_synced,
                    'message': f'Intern role {intern_role_id} ({intern_role_name}) data and {deals_synced} deals successfully updated'
                }
            else:
                return {
                    'status': 'error',
                    'intern_role_id': intern_role_id,
                    'deals_synced': deals_synced,
                    'message': f'Failed to update local intern role data for {intern_role_id}, but synced {deals_synced} deals'
                }
                
        except Exception as e:
            logger.error(f"Error processing intern role update: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def start_async_processing(self, contact_id: str, contact_info: dict):
        """
        Start asynchronous processing in a background thread to avoid blocking webhook
        """
        def async_worker():
            try:
                logger.info(f"Starting async processing for contact {contact_id}")
                
                # Get contact name
                contact_name = self.get_contact_full_name(contact_info)
                
                # Step 1: Download and manage CV files (includes duplicate cleanup)
                logger.info(f"Step 15. *********Processing CVs for contact {contact_id} ('{contact_name}') *********")
            
                downloaded_files = self.process_cv_files(contact_id, contact_name)
                
                if not downloaded_files:
                    logger.warning(f"No CV files downloaded for contact {contact_id}")
                    return
                
                # Step 2: Extract skills from downloaded CVs
                logger.info(f"Step 16. *********Extracting skills for contact {contact_id} ('{contact_name}') *********")
                skills_extracted = self.extract_skills_from_cvs(contact_id, downloaded_files)
                
                # Step 3: Trigger job matching (in one hit as requested)
                logger.info(f"Step 17. *********Matching jobs for contact {contact_id} ('{contact_name}') *********")
                match_result = match_jobs_for_contact(contact_id)
                
                logger.info(f"=== ASYNC PROCESSING COMPLETED FOR CONTACT {contact_id} ===")
                logger.info(f"  Contact Name: {contact_name}")
                logger.info(f"  CVs Downloaded: {len(downloaded_files)}")
                logger.info(f"  Skills Extracted: {skills_extracted}")
                logger.info(f"  Job Matches Created: {match_result.get('matches_created', 0)}")
                logger.info(f"  Total Job Matches: {match_result.get('total_matches', 0)}")
                
            except Exception as e:
                logger.error(f"Error in async processing for contact {contact_id}: {e}")
                # Log full stack trace for debugging
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
        
        # Start background thread
        thread = threading.Thread(target=async_worker, daemon=True)
        thread.start()
        logger.info(f"Background processing thread started for contact {contact_id}")
    
    def process_cv_files(self, contact_id: str, contact_name: str) -> List[str]:
        """
        Download CV files, removing old ones and keeping only the latest
        
        Args:
            contact_id: Contact ID
            contact_name: Contact name for file organization
            
        Returns:
            List of downloaded file paths
        """
        try:
            # Delete existing CVs for this contact to ensure single CV management
            logger.info(f"Cleaning up existing CVs for contact {contact_id}")
            self.cleanup_existing_cvs(contact_id)
            
            # Download new CVs
            logger.info(f"Downloading CVs for contact {contact_id}")
            downloaded_files = self.attachment_manager.download_contact_cvs(contact_id, contact_name)
            
            logger.info(f"Downloaded {len(downloaded_files)} CV files for contact {contact_id}")
            return downloaded_files
            
        except Exception as e:
            logger.error(f"Error processing CV files for contact {contact_id}: {e}")
            return []
    
    def cleanup_existing_cvs(self, contact_id: str):
        """
        Remove existing CV files and database records for a contact to avoid duplicates
        
        Args:
            contact_id: Contact ID to cleanup
        """
        try:
            # Get existing documents for this contact
            existing_docs = Document.objects.filter(contact_id=contact_id)
            
            if existing_docs.exists():
                logger.info(f"Found {existing_docs.count()} existing CV(s) for contact {contact_id}, cleaning up duplicates...")
                
                for doc in existing_docs:
                    # Delete physical file if it exists
                    if doc.file_path and os.path.exists(doc.file_path):
                        try:
                            os.remove(doc.file_path)
                            logger.info(f"Deleted existing CV file: {doc.file_path}")
                        except Exception as e:
                            logger.warning(f"Could not delete file {doc.file_path}: {e}")
                    
                    # Delete associated skills for this document
                    skills_deleted = Skill.objects.filter(contact_id=contact_id, document_id=doc.id).delete()
                    if skills_deleted[0] > 0:
                        logger.info(f"Deleted {skills_deleted[0]} associated skills for document {doc.id}")
                
                # Delete document records
                docs_deleted = existing_docs.delete()
                logger.info(f"Cleaned up {docs_deleted[0]} CV documents and associated skills for contact {contact_id}")
            else:
                logger.info(f"No existing CVs found for contact {contact_id}")
            
        except Exception as e:
            logger.error(f"Error cleaning up existing CVs for contact {contact_id}: {e}")
    
    def fetch_contact_from_api(self, contact_id: str) -> Optional[dict]:
        """
        Fetch complete contact data from Zoho API with rate limiting protection
        
        Args:
            contact_id: Zoho contact ID
            
        Returns:
            Contact data dictionary or None if failed
        """
        try:
            import requests
            from zoho.auth import get_access_token
            
            url = f"https://www.zohoapis.com/crm/v2/Contacts/{contact_id}"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_access_token()}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            contacts = data.get('data', [])
            
            if contacts and len(contacts) > 0:
                contact_data = contacts[0]
                logger.info(f"Successfully fetched contact {contact_id} from API")
                return contact_data
            else:
                logger.warning(f"No contact data found for {contact_id}")
                return None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 or (e.response.status_code == 400 and "too many requests" in str(e).lower()):
                logger.warning(f"Rate limited when fetching contact {contact_id} - will continue with webhook data")
                return None
            else:
                logger.error(f"HTTP error fetching contact {contact_id} from API: {e}")
                return None
        except Exception as e:
            logger.error(f"Error fetching contact {contact_id} from API: {e}")
            return None
    
    def extract_skills_from_cvs(self, contact_id: str, downloaded_files: List[str]) -> int:
        """
        Extract skills from downloaded CV files using OpenAI
        
        Args:
            contact_id: Contact ID
            downloaded_files: List of downloaded file paths
            
        Returns:
            Number of skills extracted
        """
        total_skills = 0
        
        try:
            # Get the skill extractor
            skill_extractor = self.attachment_manager.skill_extractor
            if not skill_extractor:
                logger.warning(f"Skill extractor not available for contact {contact_id}")
                return 0
            
            # Process each downloaded file
            for file_path in downloaded_files:
                try:
                    logger.info(f"Extracting skills from {file_path}")
                    
                    # Get corresponding document record
                    doc = Document.objects.get(contact_id=contact_id, file_path=file_path)
                    
                    # Extract text from PDF
                    cv_text = skill_extractor.extract_text_from_pdf(file_path)
                    if not cv_text.strip():
                        logger.warning(f"No text extracted from {file_path}")
                        continue
                    
                    # Extract skills using OpenAI
                    skills = skill_extractor.extract_skills_with_openai(cv_text)
                    if not skills:
                        logger.warning(f"No skills extracted from {file_path}")
                        continue
                    
                    # Save skills to database
                    skill_ids = skill_extractor.save_skills_to_database(skills, contact_id, doc.id)
                    total_skills += len(skill_ids)
                    
                    logger.info(f"Extracted and saved {len(skill_ids)} skills from {file_path}")
                    
                except Document.DoesNotExist:
                    logger.error(f"Document record not found for file {file_path}")
                    continue
                except Exception as e:
                    logger.error(f"Error extracting skills from {file_path}: {e}")
                    continue
            
            logger.info(f"Total skills extracted for contact {contact_id}: {total_skills}")
            return total_skills
            
        except Exception as e:
            logger.error(f"Error in skill extraction for contact {contact_id}: {e}")
            return 0
    
    def extract_contact_info(self, webhook_data: dict) -> dict:
        """
        Extract contact information from webhook payload
        
        Args:
            webhook_data: Raw webhook data
            
        Returns:
            Extracted contact information or None
        """
        try:
            # logger.info(f"Extracting contact info from: {webhook_data}")
            logger.info(f"Step 4. *********Extracting contact info from *********")
            
            # Handle form-encoded data from Zoho webhook (direct fields)
            if 'id' in webhook_data and 'name' in webhook_data:
                contact_info = {
                    'id': webhook_data.get('id'),
                    'Full_Name': webhook_data.get('name', ''),
                    'Email': webhook_data.get('email', ''),
                    'role_success_stage': webhook_data.get('role_success_stage', ''),
                    'Phone': webhook_data.get('phone', ''),
                    'Company': webhook_data.get('company', ''),
                }
                logger.info(f"Extracted from data: {contact_info}")
                return contact_info
            
            # Handle JSON webhook data structure
            if 'data' in webhook_data:
                contacts = webhook_data['data']
                if isinstance(contacts, list) and len(contacts) > 0:
                    return contacts[0]
                elif isinstance(contacts, dict):
                    return contacts
            
            # Direct contact data (JSON format)
            if 'id' in webhook_data:
                return webhook_data
            
            logger.warning("No recognizable contact format found")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting contact info: {e}")
            return None
    
    def get_contact_full_name(self, contact_data: dict) -> str:
        """
        Get full name from contact data
        
        Args:
            contact_data: Contact data dictionary
            
        Returns:
            Full name string
        """
        # Handle both JSON and form-encoded formats
        first_name = (contact_data.get('First_Name') or 
                     contact_data.get('first_name') or 
                     contact_data.get('firstName') or '')
        last_name = (contact_data.get('Last_Name') or 
                    contact_data.get('last_name') or 
                    contact_data.get('lastName') or '')
        
        # Try Full_Name field if available
        full_name = (contact_data.get('Full_Name') or 
                    contact_data.get('name') or 
                    contact_data.get('fullName'))
        if full_name:
            return str(full_name)
        
        # Construct from first and last name
        name_parts = [name for name in [first_name, last_name] if name]
        return ' '.join(name_parts) if name_parts else 'Unknown'
    
    def update_local_contact(self, contact_info: dict) -> bool:
        """
        Update contact in local database
        
        Args:
            contact_info: Contact information dictionary
            
        Returns:
            True if update was successful
        """
        try:
            # For webhook processing, we'll update the contact record directly
            # without triggering a full sync to avoid API rate limits
            contact_id = contact_info.get('id')
            
            if contact_id:
                from zoho_app.models import Contact
                
                # Try to find and update existing contact
                try:
                    contact = Contact.objects.get(id=contact_id)

                    logger.info(f"Step 7. ********* Updating existing contact {contact_id} *********")
                    
                    # Update fields if provided in webhook/API data
                    if contact_info.get('Full_Name'):
                        contact.full_name = contact_info['Full_Name']
                    if contact_info.get('First_Name') and contact_info.get('Last_Name'):
                        contact.full_name = f"{contact_info['First_Name']} {contact_info['Last_Name']}"
                    if contact_info.get('Email'):
                        contact.email = contact_info['Email']
                    if contact_info.get('Role_Success_Stage'):
                        contact.role_success_stage = contact_info['Role_Success_Stage']
                    elif contact_info.get('role_success_stage'):
                        contact.role_success_stage = contact_info['role_success_stage']
                    if contact_info.get('Phone'):
                        contact.phone = contact_info['Phone']
                    if contact_info.get('Mobile'):
                        contact.mobile = contact_info['Mobile']
                    if contact_info.get('Company') or contact_info.get('Account_Name'):
                        company = contact_info.get('Company') or contact_info.get('Account_Name', {}).get('name') if isinstance(contact_info.get('Account_Name'), dict) else contact_info.get('Account_Name')
                        if company:
                            contact.company = company
                    if contact_info.get('Title'):
                        contact.title = contact_info['Title']
                    if contact_info.get('Department'):
                        contact.department = contact_info['Department']
                    if contact_info.get('Lead_Source'):
                        contact.lead_source = contact_info['Lead_Source']
                    if contact_info.get('Mailing_Street'):
                        contact.mailing_address = f"{contact_info.get('Mailing_Street', '')} {contact_info.get('Mailing_City', '')} {contact_info.get('Mailing_State', '')}"
                    
                    # Update timestamp
                    contact.updated_time = timezone.now()
                    contact.save()
                    logger.info(f"Step 8. *********Successfully updated local contact {contact_id} *********")
                    
                except Contact.DoesNotExist:
                
                    logger.info(f"Step 7. *********Contact {contact_id} not found locally - creating new record *********")
                    
                    # Prepare contact data for creation
                    full_name = contact_info.get('Full_Name')
                    if not full_name and contact_info.get('First_Name') and contact_info.get('Last_Name'):
                        full_name = f"{contact_info['First_Name']} {contact_info['Last_Name']}"
                    
                    company = contact_info.get('Company')
                    if not company and contact_info.get('Account_Name'):
                        if isinstance(contact_info['Account_Name'], dict):
                            company = contact_info['Account_Name'].get('name')
                        else:
                            company = contact_info['Account_Name']
                    
                    role_success_stage = contact_info.get('Role_Success_Stage') or contact_info.get('role_success_stage', '')
                    
                    mailing_address = ""
                    if contact_info.get('Mailing_Street'):
                        mailing_address = f"{contact_info.get('Mailing_Street', '')} {contact_info.get('Mailing_City', '')} {contact_info.get('Mailing_State', '')}"
                    
                    # Create new contact record
                    contact = Contact.objects.create(
                        id=contact_id,
                        full_name=full_name or '',
                        email=contact_info.get('Email', ''),
                        placement_automation=contact_info.get('Placement_Automation') or contact_info.get('placement_automation'),
                        role_success_stage=role_success_stage,
                        phone=contact_info.get('Phone', ''),
                        mobile=contact_info.get('Mobile', ''),
                        company=company or '',
                        title=contact_info.get('Title', ''),
                        department=contact_info.get('Department', ''),
                        lead_source=contact_info.get('Lead_Source', ''),
                        mailing_address=mailing_address,
                        created_time=timezone.now(),
                        updated_time=timezone.now()
                    )
                    logger.info(f"Step 8. *********Successfully created new local contact {contact_id} *********")

                return True
            else:
                logger.warning("No contact ID provided for local update")
                return False
                
        except Exception as e:
            logger.error(f"Error updating local contact: {e}")
            return False
    
    def sync_related_account(self, account_id: str) -> bool:
        """
        Sync related account data from Zoho API
        
        Args:
            account_id: Zoho account ID
            
        Returns:
            True if sync was successful
        """
        try:
            logger.info(f"Step 10. *********Syncing account data for account {account_id} *********")
            
            # Fetch account data from Zoho API
            account_data = self.fetch_account_from_api(account_id)
            if not account_data:
                logger.warning(f"Could not fetch account {account_id} from API")
                return False
            
            # Update local account data
            self.update_local_account(account_data)
            logger.info(f"Step 11. *********Successfully synced account and update local data {account_id} *********")
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing account {account_id}: {e}")
            return False
    
    def fetch_account_from_api(self, account_id: str) -> Optional[dict]:
        """
        Fetch account data from Zoho API with rate limiting protection
        
        Args:
            account_id: Zoho account ID
            
        Returns:
            Account data dictionary or None if failed
        """
        try:
            import requests
            from zoho.auth import get_access_token
            
            url = f"https://www.zohoapis.com/crm/v2/Accounts/{account_id}"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_access_token()}",
                "Content-Type": "application/json"
            }

            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            accounts = data.get('data', [])
            
            if accounts and len(accounts) > 0:
                account_data = accounts[0]
                logger.info(f"Successfully fetched account {account_id} from API")
                return account_data
            else:
                logger.warning(f"No account data found for {account_id}")
                return None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 or (e.response.status_code == 400 and "too many requests" in str(e).lower()):
                logger.warning(f"Rate limited when fetching account {account_id} - skipping account sync")
                return None
            else:
                logger.error(f"HTTP error fetching account {account_id} from API: {e}")
                return None
        except Exception as e:
            logger.error(f"Error fetching account {account_id} from API: {e}")
            return None
    
    def update_local_account(self, account_info: dict) -> bool:
        """
        Update account in local database with comprehensive field mapping
        
        Args:
            account_info: Account information dictionary from Zoho API
            
        Returns:
            True if update was successful
        """
        try:
            from zoho_app.models import Account
            from django.utils.dateparse import parse_datetime
            
            account_id = account_info.get('id')
            if not account_id:
                logger.warning("No account ID provided for local update")
                return False
            
            # Helper function to parse date fields
            def parse_date_field(date_str):
                if date_str:
                    try:
                        return parse_datetime(date_str)
                    except:
                        return None
                return None
            
            # Helper function to safely get boolean values
            def get_bool_value(value):
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ['true', '1', 'yes']
                return False
            
            # Prepare account data with comprehensive field mapping
            account_data = {
                'id': account_id,
                'name': account_info.get('Account_Name') or account_info.get('name'),
                'industry': account_info.get('Industry'),
                'billing_address': account_info.get('Billing_Street'),
                'shipping_address': account_info.get('Shipping_Street'),
                
                # Owner information
                'owner_id': account_info.get('Owner', {}).get('id') if isinstance(account_info.get('Owner'), dict) else account_info.get('Owner'),
                'owner_name': account_info.get('Owner', {}).get('name') if isinstance(account_info.get('Owner'), dict) else None,
                'owner_email': account_info.get('Owner', {}).get('email') if isinstance(account_info.get('Owner'), dict) else None,
                
                # Date fields
                'cleanup_start_date': parse_date_field(account_info.get('Cleanup_Start_Date')),
                'last_activity_time': parse_date_field(account_info.get('Last_Activity_Time')),
                'last_full_due_diligence_date': parse_date_field(account_info.get('Last_Full_Due_Diligence_Date')),
                'follow_up_date': parse_date_field(account_info.get('Follow_up_Date')),
                'next_reply_date': parse_date_field(account_info.get('Next_Reply_Date')),
                
                # Text fields
                'field_states': account_info.get('$field_states'),
                'management_status': account_info.get('Management_Status'),
                'company_work_policy': account_info.get('Company_Work_Policy'),
                'company_industry': account_info.get('Company_Industry'),
                'company_description': account_info.get('Company_Description'),
                'approval_status': account_info.get('Approval_Status'),
                'street': account_info.get('Street'),
                'classic_partnership': account_info.get('Classic_Partnership'),
                'state_region': account_info.get('State_Region'),
                'cleanup_status': account_info.get('Cleanup_Status'),
                'uni_region': account_info.get('Uni_Region'),
                'approval': account_info.get('Approval'),
                'uni_outreach_status': account_info.get('Uni_Outreach_Status'),
                'enrich_status': account_info.get('Enrich_Status'),
                'roles_available': account_info.get('Roles_Available'),
                'roles': account_info.get('Roles'),
                'city': account_info.get('City'),
                'postcode': account_info.get('Postcode'),
                'outreach_notes': account_info.get('Outreach_Notes'),
                'company_industry_other': account_info.get('Company_Industry_Other'),
                'no_employees': account_info.get('No_Employees'),
                'industry_areas': account_info.get('Industry_Areas'),
                'placement_revision_required': account_info.get('Placement_Revision_Required'),
                'country': account_info.get('Country'),
                'uni_state_if_in_us': account_info.get('Uni_State_if_in_US'),
                'review_process': account_info.get('Review_Process'),
                'layout_id': account_info.get('$layout_id'),
                'layout_display_label': account_info.get('$layout_display_label'),
                'layout_name': account_info.get('$layout_name'),
                'review': account_info.get('Review'),
                'cleanup_notes': account_info.get('Cleanup_Notes'),
                'account_notes': account_info.get('Account_Notes'),
                'standard_working_hours': account_info.get('Standard_Working_Hours'),
                'due_diligence_fields_to_revise': account_info.get('Due_Diligence_Fields_to_Revise'),
                'uni_country': account_info.get('Uni_Country'),
                'cleanup_phase': account_info.get('Cleanup_Phase'),
                'record_status': account_info.get('Record_Status'),
                'type': account_info.get('Type'),
                'uni_timezone': account_info.get('Uni_Timezone'),
                'company_address': account_info.get('Company_Address'),
                'tag': account_info.get('Tag'),
                'approval_state': account_info.get('$approval_state'),
                'location': account_info.get('Location'),
                'location_other': account_info.get('Location_Other'),
                'account_status': account_info.get('Account_Status'),
                
                # Boolean fields
                'process_flow': get_bool_value(account_info.get('$process_flow')),
                'locked_for_me': get_bool_value(account_info.get('$locked_for_me')),
                'is_duplicate': get_bool_value(account_info.get('$is_duplicate')),
                'in_merge': get_bool_value(account_info.get('$in_merge')),
                'upon_to_remote_interns': get_bool_value(account_info.get('Upon_to_Remote_Interns')),
                'locked': get_bool_value(account_info.get('$locked')),
                'is_dnc': get_bool_value(account_info.get('$is_dnc')),
                'pathfinder': get_bool_value(account_info.get('Pathfinder')),
                'gold_rating': get_bool_value(account_info.get('Gold_Rating')),
            }
            
            # Try to find and update existing account
            try:
                account = Account.objects.get(id=account_id)
                logger.info(f"Updating existing account {account_id}")
                
                # Update all fields
                for field_name, field_value in account_data.items():
                    if field_name != 'id' and field_value is not None:
                        setattr(account, field_name, field_value)
                
                account.save()
                logger.info(f"Successfully updated local account {account_id}")
                
            except Account.DoesNotExist:
                logger.info(f"Account {account_id} not found locally - creating new record")
                
                # Create new account record
                account = Account.objects.create(**account_data)
                logger.info(f"Created new local account {account_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating local account: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def sync_intern_roles_incremental(self) -> bool:
        """
        Trigger incremental sync for intern roles to keep job data fresh
        
        Returns:
            True if sync was successful
        """
        try:
            logger.info("Starting incremental sync for intern roles")
            
            # Use the existing ETL pipeline for incremental sync
            sync_intern_roles(incremental=True)
            logger.info(f"Step 13. *********Incremental sync for intern roles completed *********")
            return True
            
        except Exception as e:
            logger.error(f"Error in incremental sync for intern roles: {e}")
            return False
    
    def sync_account_deals(self, account_id: str) -> int:
        """
        Sync deals associated with an account from Zoho API
        
        Args:
            account_id: Zoho account ID
            
        Returns:
            Number of deals synced
        """
        try:
            logger.info(f"Fetching deals for account {account_id}")
            
            # Fetch deals for this account from Zoho API
            deals_data = self.fetch_account_deals_from_api(account_id)
            
            if not deals_data:
                logger.info(f"No deals found for account {account_id}")
                return 0
            
            deals_synced = 0
            for deal_data in deals_data:
                try:
                    if self.update_local_deal(deal_data):
                        deals_synced += 1
                except Exception as e:
                    logger.error(f"Error updating deal {deal_data.get('id', 'unknown')}: {e}")
                    continue
            
            logger.info(f"Successfully synced {deals_synced} deals for account {account_id}")
            return deals_synced
            
        except Exception as e:
            logger.error(f"Error syncing deals for account {account_id}: {e}")
            return 0
    
    def fetch_account_deals_from_api(self, account_id: str) -> List[dict]:
        """
        Fetch deals associated with an account from Zoho API
        
        Args:
            account_id: Zoho account ID
            
        Returns:
            List of deal data dictionaries
        """
        try:
            from zoho.auth import get_access_token
            
            # Use the search API to find deals by account ID
            url = "https://www.zohoapis.com/crm/v2/Deals/search"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_access_token()}",
                "Content-Type": "application/json"
            }
            
            # Search for deals with the specific account ID
            params = {
                "criteria": f"Account_Name:equals:{account_id}",
                "per_page": 200  # Maximum allowed per page
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            deals = data.get('data', [])
            
            logger.info(f"Found {len(deals)} deals for account {account_id}")
            return deals
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limited when fetching deals for account {account_id}")
                return []
            else:
                logger.error(f"HTTP error fetching deals for account {account_id}: {e}")
                return []
        except Exception as e:
            logger.error(f"Error fetching deals for account {account_id}: {e}")
            return []
    
    def update_local_deal(self, deal_info: dict) -> bool:
        """
        Update deal in local database with comprehensive field mapping
        
        Args:
            deal_info: Deal information dictionary from Zoho API
            
        Returns:
            True if update was successful
        """
        try:
            from zoho_app.models import Deal
            from django.utils.dateparse import parse_datetime
            
            deal_id = deal_info.get('id')
            if not deal_id:
                logger.warning("No deal ID provided for local update")
                return False
            
            # Helper function to parse date fields
            def parse_date_field(date_str):
                if date_str:
                    try:
                        return parse_datetime(date_str)
                    except:
                        return None
                return None
            
            # Prepare deal data with comprehensive field mapping
            deal_data = {
                'id': deal_id,
                'deal_name': deal_info.get('Deal_Name'),
                'description': deal_info.get('Description'),
                'account_id': deal_info.get('Account_Name', {}).get('id') if isinstance(deal_info.get('Account_Name'), dict) else deal_info.get('Account_Name'),
                'account_name': deal_info.get('Account_Name', {}).get('name') if isinstance(deal_info.get('Account_Name'), dict) else None,
                'stage': deal_info.get('Stage'),
                'start_date': parse_date_field(deal_info.get('Start_Date')),
                'end_date': parse_date_field(deal_info.get('End_Date')),
                'created_time': parse_date_field(deal_info.get('Created_Time')),
                'modified_time': parse_date_field(deal_info.get('Modified_Time')),
            }
            
            # Try to find and update existing deal
            try:
                deal = Deal.objects.get(id=deal_id)
                logger.info(f"Updating existing deal {deal_id}")
                
                # Update all fields
                for field_name, field_value in deal_data.items():
                    if field_name != 'id' and field_value is not None:
                        setattr(deal, field_name, field_value)
                
                deal.save()
                logger.info(f"Successfully updated local deal {deal_id}")
                
            except Deal.DoesNotExist:
                logger.info(f"Deal {deal_id} not found locally - creating new record")
                
                # Create new deal record
                deal = Deal.objects.create(**deal_data)
                logger.info(f"Created new local deal {deal_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating local deal: {e}")
            return False
    
    def fetch_intern_role_from_api(self, intern_role_id: str) -> Optional[dict]:
        """
        Fetch intern role data from Zoho API with rate limiting protection
        
        Args:
            intern_role_id: Zoho intern role ID
            
        Returns:
            Intern role data dictionary or None if failed
        """
        try:
            from zoho.auth import get_access_token
            
            url = f"https://www.zohoapis.com/crm/v2/Intern_Roles/{intern_role_id}"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_access_token()}",
                "Content-Type": "application/json"
            }

            response = requests.get(url, headers=headers, timeout=120)
            response.raise_for_status()
            
            data = response.json()
            roles = data.get('data', [])
            
            if roles and len(roles) > 0:
                role_data = roles[0]
                logger.info(f"Successfully fetched intern role {intern_role_id} from API")
                return role_data
            else:
                logger.warning(f"No intern role data found for {intern_role_id}")
                return None
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 or (e.response.status_code == 400 and "too many requests" in str(e).lower()):
                logger.warning(f"Rate limited when fetching intern role {intern_role_id} - skipping role sync")
                return None
            else:
                logger.error(f"HTTP error fetching intern role {intern_role_id} from API: {e}")
                return None
        except Exception as e:
            logger.error(f"Error fetching intern role {intern_role_id} from API: {e}")
            return None
    
    def update_local_intern_role(self, role_info: dict) -> bool:
        """
        Update intern role in local database with comprehensive field mapping
        
        Args:
            role_info: Intern role information dictionary from Zoho API
            
        Returns:
            True if update was successful
        """
        try:
            from zoho_app.models import InternRole
            from django.utils.dateparse import parse_datetime
            
            role_id = role_info.get('id')
            if not role_id:
                logger.warning("No intern role ID provided for local update")
                return False
            
            # Helper function to parse date fields
            def parse_date_field(date_str):
                if date_str:
                    try:
                        return parse_datetime(date_str)
                    except:
                        return None
                return None
            
            # Helper function to safely get boolean values
            def get_bool_value(value):
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ['true', '1', 'yes']
                return False
            
            # Prepare intern role data with comprehensive field mapping
            role_data = {
                'id': role_id,
                'name': role_info.get('Name'),
                'role_title': role_info.get('Role_Title'),
                'role_description_requirements': role_info.get('Role_Description_Requirements'),
                'role_status': role_info.get('Role_Status'),
                'role_function': role_info.get('Role_Function'),
                'role_department_size': role_info.get('Role_Department_Size'),
                'role_attachments_jd': role_info.get('Role_Attachments_JD'),
                'role_tags': role_info.get('Role_Tags'),
                'start_date': parse_date_field(role_info.get('Start_Date')),
                'end_date': parse_date_field(role_info.get('End_Date')),
                'created_time': parse_date_field(role_info.get('Created_Time')),
                
                # Company information
                'intern_company_id': role_info.get('Intern_Company', {}).get('id') if isinstance(role_info.get('Intern_Company'), dict) else role_info.get('Intern_Company'),
                'intern_company_name': role_info.get('Intern_Company', {}).get('name') if isinstance(role_info.get('Intern_Company'), dict) else None,
                
                # Work and location fields
                'company_work_policy': role_info.get('Company_Work_Policy'),
                'location': role_info.get('Location'),
                'open_to_remote': role_info.get('Open_to_Remote'),
                'due_diligence_status_2': role_info.get('Due_Diligence_Status_2'),
                'account_outreach_status': role_info.get('Account_Outreach_Status'),
                'record_status': role_info.get('Record_Status'),
                'approval_state': role_info.get('$approval_state'),
                'management_status': role_info.get('Management_Status'),
                'placement_fields_to_revise': role_info.get('Placement_Fields_to_Revise'),
                'placement_revision_notes': role_info.get('Placement_Revision_Notes'),
                
                # Boolean fields
                'gold_rating': get_bool_value(role_info.get('Gold_Rating')),
                'locked': get_bool_value(role_info.get('$locked')),
            }
            
            # Try to find and update existing intern role
            try:
                role = InternRole.objects.get(id=role_id)
                logger.info(f"Updating existing intern role {role_id}")
                
                # Update all fields
                for field_name, field_value in role_data.items():
                    if field_name != 'id' and field_value is not None:
                        setattr(role, field_name, field_value)
                
                role.save()
                logger.info(f"Successfully updated local intern role {role_id}")
                
            except InternRole.DoesNotExist:
                logger.info(f"Intern role {role_id} not found locally - creating new record")
                
                # Create new intern role record
                role = InternRole.objects.create(**role_data)
                logger.info(f"Created new local intern role {role_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating local intern role: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def sync_intern_role_deals(self, intern_role_id: str) -> int:
        """
        Sync deals for a specific intern role using the existing job matcher functionality
        
        Args:
            intern_role_id: Intern role ID
            
        Returns:
            Number of deals synced
        """
        try:
            from etl.job_matcher import JobMatcher
            
            # Create job matcher instance
            job_matcher = JobMatcher()
            
            # Use the existing sync_role_deals method
            deals_count = job_matcher.sync_role_deals(intern_role_id)
            
            logger.info(f"Successfully synced {deals_count} deals for intern role {intern_role_id}")
            return deals_count
            
        except Exception as e:
            logger.error(f"Error syncing deals for intern role {intern_role_id}: {e}")
            return 0
    
    def sync_specific_contacts(self, contact_ids: List[str]) -> dict:
        """
        Sync specific contacts by their IDs
        
        Args:
            contact_ids: List of contact IDs to sync
            
        Returns:
            Sync results dictionary
        """
        results = {
            'total_requested': len(contact_ids),
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        for contact_id in contact_ids:
            try:
                logger.info(f"Syncing specific contact: {contact_id}")
                
                # Fetch latest data from API
                contact_data = self.fetch_contact_from_api(contact_id)
                if contact_data:
                    # Update local data
                    self.update_local_contact(contact_data)
                    results['successful'] += 1
                    logger.info(f"Successfully synced contact {contact_id}")
                else:
                    results['failed'] += 1
                    results['errors'].append(f"Could not fetch contact {contact_id}")
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"Error syncing contact {contact_id}: {str(e)}")
                logger.error(f"Error syncing contact {contact_id}: {e}")
        
        return results
    
    def sync_specific_accounts(self, account_ids: List[str]) -> dict:
        """
        Sync specific accounts by their IDs
        
        Args:
            account_ids: List of account IDs to sync
            
        Returns:
            Sync results dictionary
        """
        results = {
            'total_requested': len(account_ids),
            'successful': 0,
            'failed': 0,
            'errors': []
        }
        
        for account_id in account_ids:
            try:
                logger.info(f"Syncing specific account: {account_id}")
                
                # Fetch latest data from API
                account_data = self.fetch_account_from_api(account_id)
                if account_data:
                    # Update local data
                    self.update_local_account(account_data)
                    results['successful'] += 1
                    logger.info(f"Successfully synced account {account_id}")
                else:
                    results['failed'] += 1
                    results['errors'].append(f"Could not fetch account {account_id}")
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"Error syncing account {account_id}: {str(e)}")
                logger.error(f"Error syncing account {account_id}: {e}")
        
        return results



# Simple PK-listing view for accounts / contacts / intern roles
from django.shortcuts import render

def pk_list_tabs_view(request):
    """
    Render a simple page with three tabs (accounts, contacts, intern_roles).
    Supports filtering by exact pk or range (pk_min, pk_max) and pagination.
    Query params:
      - tab: accounts|contacts|intern_roles (default: contacts)
      - pk: exact pk value
      - pk_min, pk_max: range filter (inclusive)
      - page: page number
      - per_page: items per page
    """
    tab = request.GET.get('tab', 'contacts')
    pk = request.GET.get('pk')
    pk_min = request.GET.get('pk_min')
    pk_max = request.GET.get('pk_max')
    try:
        per_page = int(request.GET.get('per_page', 25))
    except Exception:
        per_page = 25
    page = request.GET.get('page', 1)

    if tab == 'accounts':
        qs = Account.objects.all().order_by('id')
    elif tab == 'intern_roles':
        qs = InternRole.objects.all().order_by('id')
    else:
        tab = 'contacts'
        qs = Contact.objects.all().order_by('id')

    # Apply PK filters. Since PKs are strings, use lexicographical filters which work
    # for most cases. Exact match when 'pk' provided.
    if pk:
        qs = qs.filter(pk=pk)
    else:
        if pk_min:
            qs = qs.filter(id__gte=pk_min)
        if pk_max:
            qs = qs.filter(id__lte=pk_max)

    paginator = Paginator(qs, per_page)
    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    context = {
        'tab': tab,
        'items': page_obj.object_list,
        'page_obj': page_obj,
        'per_page': per_page,
        'request': request,
    }
    return render(request, 'zoho_app/pk_list_tabs.html', context)
# Lazy initialization of webhook handler
webhook_handler = None

def get_webhook_handler():
    """Get webhook handler instance with lazy initialization"""
    global webhook_handler
    if webhook_handler is None:
        webhook_handler = ZohoWebhookHandler()
    return webhook_handler


@csrf_exempt
@require_http_methods(["POST"])
def handle_contact_webhook(request):
    """Handle Zoho contact webhook notifications"""
    try:

        logger.info(f"Step 1. *********Webhook trigger received *********")
        # Parse request body based on content type
        webhook_data = None
        if request.content_type == 'application/json':
            webhook_data = json.loads(request.body.decode('utf-8'))
        elif request.content_type.startswith('application/x-www-form-urlencoded'):
            # Parse form-encoded data from Zoho
            from urllib.parse import parse_qs, unquote
            raw_data = request.body.decode('utf-8')
            parsed_data = parse_qs(raw_data)
            
            # Convert to single values and decode URL encoding
            webhook_data = {}
            for key, values in parsed_data.items():
                webhook_data[key] = unquote(values[0]) if values else ''
            
            logger.info(f"Step 2. *********Parsed form data received *********")
        else:
            # logger.error(f"Unsupported content type: {request.content_type}")
            return JsonResponse({'error': 'Unsupported content type'}, status=400)
        
        logger.info(f"Step 3. *********Parsed webhook data received *********")
        logger.info(f"data: {json.dumps(webhook_data, indent=2)[:500]}...")
        
        # Verify signature if provided
        signature = request.headers.get('X-Zoho-Signature')
        if signature:
            handler = get_webhook_handler()
            if not handler.verify_webhook_signature(request.body.decode('utf-8'), signature):
                logger.warning("Invalid webhook signature")
                return JsonResponse({'error': 'Invalid signature'}, status=401)
        
        # Process the webhook
        handler = get_webhook_handler()
        result = handler.process_contact_update(webhook_data)
        
        logger.info(f"Webhook processing result: {result}")
        
        if result['status'] in ['success', 'skipped']:
            return JsonResponse(result, status=200)
        else:
            logger.warning(f"Webhook processing failed: {result}")
            return JsonResponse(result, status=400)
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': 'Invalid JSON payload'}, status=400)
    except Exception as e:
        logger.error(f"Webhook handling error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def handle_account_webhook(request):
    """Handle Zoho account webhook notifications"""
    try:
        logger.info(f"Step 1. *********Webhook account trigger received *********")
        
        # Parse request body based on content type
        webhook_data = None
        if request.content_type.startswith('application/x-www-form-urlencoded'):
            # Parse form-encoded data from Zoho
            from urllib.parse import parse_qs, unquote
            raw_data = request.body.decode('utf-8')
            parsed_data = parse_qs(raw_data)
            
            # Convert to single values and decode URL encoding
            webhook_data = {}
            for key, values in parsed_data.items():
                webhook_data[key] = unquote(values[0]) if values else ''
            
            logger.info(f"Step 2. *********Parsed form data received *********")
        else:
            logger.error(f"Unsupported content type: {request.content_type}")
            return JsonResponse({'error': 'Unsupported content type'}, status=400)
        
        logger.info(f"Step 3. *********Parsed webhook data received *********")
        logger.info(f"data: {json.dumps(webhook_data, indent=2)[:500]}...")
        
        # Extract account ID from webhook data
        account_id = webhook_data.get('id')
        account_name = webhook_data.get('name', 'Unknown')
        
        if not account_id:
            logger.error("No account ID found in webhook data")
            return JsonResponse({'error': 'No account ID provided'}, status=400)
        
        logger.info(f"Step 4. *********Processing account webhook for ID: {account_id}, Name: {account_name} *********")
        
        # Process the account webhook
        handler = get_webhook_handler()
        result = handler.process_account_update(webhook_data)
        
        logger.info(f"Account webhook processing result: {result}")
        
        if result['status'] == 'success':
            return JsonResponse(result, status=200)
        else:
            logger.warning(f"Account webhook processing failed: {result}")
            return JsonResponse(result, status=400)
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': 'Invalid JSON payload'}, status=400)
    except Exception as e:
        logger.error(f"Account webhook handling error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def handle_intern_role_webhook(request):
    """Handle Zoho intern role webhook notifications"""
    try:
        logger.info(f"Step 1. *********Webhook intern role trigger received *********")

        # Parse request body based on content type
        webhook_data = None
        if request.content_type.startswith('application/x-www-form-urlencoded'):
            # Parse form-encoded data from Zoho
            from urllib.parse import parse_qs, unquote
            raw_data = request.body.decode('utf-8')
            parsed_data = parse_qs(raw_data)
            
            # Convert to single values and decode URL encoding
            webhook_data = {}
            for key, values in parsed_data.items():
                webhook_data[key] = unquote(values[0]) if values else ''
            
            logger.info(f"Step 2. *********Parsed form data received *********")
        elif request.content_type == 'application/json':
            webhook_data = json.loads(request.body.decode('utf-8'))
        else:
            logger.error(f"Unsupported content type: {request.content_type}")
            return JsonResponse({'error': 'Unsupported content type'}, status=400)
        
        logger.info(f"Step 3. *********Parsed webhook data received *********")
        logger.info(f"data: {json.dumps(webhook_data, indent=2)[:500]}...")
        
        # Extract intern role ID from webhook data
        intern_role_id = webhook_data.get('id')
        intern_role_name = webhook_data.get('name') or webhook_data.get('Role_Title', 'Unknown')
        
        if not intern_role_id:
            logger.error("No intern role ID found in webhook data")
            return JsonResponse({'error': 'No intern role ID provided'}, status=400)
        
        logger.info(f"Step 4. *********Processing intern role webhook for ID: {intern_role_id}, Name: {intern_role_name} *********")
        
        # Process the intern role webhook
        handler = get_webhook_handler()
        result = handler.process_intern_role_update(webhook_data)
        
        logger.info(f"Intern role webhook processing result: {result}")
        
        if result['status'] == 'success':
            return JsonResponse(result, status=200)
        else:
            logger.warning(f"Intern role webhook processing failed: {result}")
            return JsonResponse(result, status=400)
            
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': 'Invalid JSON payload'}, status=400)
    except Exception as e:
        logger.error(f"Intern role webhook handling error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': str(e)}, status=500)

def sync_single_contact(contact_id):
    """
    Sync a single contact from Zoho CRM to local database
    
    Args:
        contact_id: The Zoho contact ID to sync
        
    Returns:
        dict: Result dictionary with status and message
    """
    try:
        from etl.pipeline import parse_datetime_field, extract_nested_name, list_to_json_string
        
        logger.info(f"Starting sync for contact {contact_id}")
        
        # Initialize Zoho client
        zoho_client = ZohoClient()
        
        # Fetch contact data from Zoho API
        contact_data = zoho_client.get_contact_by_id(contact_id)
        if not contact_data:
            return {
                'status': 'error',
                'message': f'Contact {contact_id} not found in Zoho CRM'
            }
        
        logger.info(f"Fetched contact data from Zoho for {contact_id}")
        
        # Map contact fields to model fields (using the same mapping logic as the ETL pipeline)
        with transaction.atomic():
            contact_fields_mapped = {
                # Core fields
                'id': contact_data.get('id'),
                'email': contact_data.get('Email'),
                'first_name': contact_data.get('First_Name'),
                'last_name': contact_data.get('Last_Name'),
                'phone': contact_data.get('Phone'),
                'account_id': contact_data.get('Account_Name', {}).get('id') if isinstance(contact_data.get('Account_Name'), dict) else None,
                'account_name': extract_nested_name(contact_data.get('Account_Name')),
                'title': contact_data.get('Title'),
                'department': contact_data.get('Department'),
                'updated_time': parse_datetime_field(contact_data.get('Modified_Time')),
                'created_time': parse_datetime_field(contact_data.get('Created_Time')),
                'placement_automation': contact_data.get('Placement_Automation') or contact_data.get('placement_automation'),
                'full_name': contact_data.get('Full_Name'),
                
                # Location and Industry fields
                'location': contact_data.get('Location'),
                'industry': contact_data.get('Industry'),
                'industry_choice_1': contact_data.get('Industry_Choice_1'),
                'industry_choice_2': contact_data.get('Industry_choice_2'),
                'industry_choice_3': contact_data.get('Industry_Choice_3'),
                'industry_1_areas': contact_data.get('Industry_1_Areas'),
                'industry_2_areas': contact_data.get('Industry_2_Areas'),
                'current_location_v2': contact_data.get('Current_Location_V2'),
                'location_other': contact_data.get('Location_Other'),
                'alternative_location1': contact_data.get('Alternative_Location1'),
                'country_city_of_residence': contact_data.get('Country_city_of_residence'),
                
                # Student and Academic fields
                'skills': contact_data.get('Skills'),
                'student_status': contact_data.get('Student_Status'),
                'university_name': contact_data.get('University_Name'),
                'graduation_date': parse_datetime_field(contact_data.get('Graduation_Date')),
                'student_bio': contact_data.get('Student_Bio'),
                'uni_start_date': parse_datetime_field(contact_data.get('Uni_Start_Date')),
                'english_level': contact_data.get('English_Level'),
                'age_on_start_date': contact_data.get('Age_on_Start_Date'),
                'date_of_birth': parse_datetime_field(contact_data.get('Date_of_Birth')),
                
                # Placement and Role fields
                'placement_status': contact_data.get('Placement_status'),
                'start_date': parse_datetime_field(contact_data.get('Start_date')),
                'end_date': parse_datetime_field(contact_data.get('End_date')),
                'role_success_stage': contact_data.get('Role_Success_Stage'),
                'role_owner': extract_nested_name(contact_data.get('Role_Owner')),
                'role_success_notes': contact_data.get('Role_Success_Notes'),
                'role_confirmed_date': parse_datetime_field(contact_data.get('Role_confirmed_date')),
                'paid_role': contact_data.get('Paid_Role'),
                'likelihood_to_convert': contact_data.get('Likelihood_to_convert'),
                'job_title': contact_data.get('Job_Title'),
                'job_offered_after': contact_data.get('Job_offered_after'),
                
                # Contact and Communication fields
                'link_to_cv': contact_data.get('Link_to_CV'),
                'contact_email': contact_data.get('Contact_Email'),
                'secondary_email': contact_data.get('Secondary_Email'),
                'do_not_contact': contact_data.get('Do_Not_Contact', False),
                'email_opt_out': contact_data.get('Email_Opt_Out', False),
                'unsubscribed_time': parse_datetime_field(contact_data.get('Unsubscribed_Time')),
                'follow_up_date': parse_datetime_field(contact_data.get('Follow_up_Date')),
                
                # Personal Information
                'gender': contact_data.get('Gender'),
                'nationality': contact_data.get('Nationality'),
                'timezone': contact_data.get('Timezone'),
                'contact_last_name': contact_data.get('Contact_Last_Name'),
                
                # Visa and Travel fields
                'visa_eligible': contact_data.get('Visa_Eligible'),
                'requires_a_visa': contact_data.get('Requires_a_visa'),
                'visa_type_exemption': contact_data.get('Visa_Type_Exemption'),
                'visa_successful': contact_data.get('Visa_successful'),
                'visa_alt_options': list_to_json_string(contact_data.get('Visa_Alt_Options')),
                'visa_notes': contact_data.get('Visa_Note_s'),
                'visa_owner': extract_nested_name(contact_data.get('Visa_Owner')),
                'visa_f_u_date': parse_datetime_field(contact_data.get('Visa_F_U_Date')),
                'arrival_date_time': parse_datetime_field(contact_data.get('Arrival_date_time')),
                'departure_date_time': parse_datetime_field(contact_data.get('Departure_date_time')),
                'departure_flight_number': contact_data.get('Departure_flight_number'),
                'arrival_drop_off_address': contact_data.get('Arrival_drop_off_address'),
                
                # Additional fields to complete the mapping
                'interview': contact_data.get('Interview'),
                'interview_successful': contact_data.get('Interview_successful'),
                'interviewer': contact_data.get('Interviewer'),
                'myinterview_url': contact_data.get('MyInterview_URL'),
                'intro_call_date': parse_datetime_field(contact_data.get('Intro_Call_Date')),
                'call_scheduled_date_time': parse_datetime_field(contact_data.get('Call_Scheduled_Date_Time')),
                'call_booked_date_time': parse_datetime_field(contact_data.get('Call_Booked_Date_Time')),
                'call_to_conversion_time_days': contact_data.get('Call_to_Conversion_Time_days'),
                'enrolment_to_intro_call_lead_time': contact_data.get('Enrolment_to_Intro_Call_Lead_Time'),
                'process_flow': contact_data.get('$process_flow'),
                'approval': contact_data.get('$approval'),
                'approval_date': parse_datetime_field(contact_data.get('Approval_date')),
                'approval_state': contact_data.get('$approval_state'),
                'review_process': contact_data.get('$review_process'),
                'admission_member': extract_nested_name(contact_data.get('Admission_Member')),
                'lead_created_time': parse_datetime_field(contact_data.get('Lead_Created_Time')),
                'last_activity_time': parse_datetime_field(contact_data.get('Last_Activity_Time')),
                'layout_id': contact_data.get('Layout', {}).get('id') if isinstance(contact_data.get('Layout'), dict) else None,
                'layout_display_label': contact_data.get('Layout', {}).get('display_label') if isinstance(contact_data.get('Layout'), dict) else None,
                'layout_name': contact_data.get('Layout', {}).get('name') if isinstance(contact_data.get('Layout'), dict) else None,
                'field_states': contact_data.get('$field_states'),
                'student_decision': contact_data.get('Student_decision'),
                'company_decision': contact_data.get('Company_decision'),
                'rating_new': contact_data.get('Rating_New'),
                'rating': contact_data.get('Rating'),
                'warm_call': contact_data.get('Warm_Call'),
                'other_industry': contact_data.get('Other_industry'),
                'alternative_location1': contact_data.get('Alternative_Location1'),
                'review': contact_data.get('$review'),
                'reason_for_cancellation': contact_data.get('Reason_for_Cancellation'),
                'cancelled_date_time': parse_datetime_field(contact_data.get('Cancelled_Date_Time')),
                'notes1': contact_data.get('Notes1'),
                'partner_organisation': contact_data.get('Partner_Organisation'),
                'date_of_cancellation': parse_datetime_field(contact_data.get('Date_of_Cancellation')),
                'in_merge': contact_data.get('$in_merge'),
                'duration': contact_data.get('Duration'),
                'utm_campaign': contact_data.get('UTM_Campaign'),
                'utm_medium': contact_data.get('UTM_Medium'),
                'utm_content': contact_data.get('UTM_Content'),
                'utm_gclid': contact_data.get('UTM_GCLID'),
                'description': contact_data.get('Description'),
                'industry_2_areas': contact_data.get('Industry_2_Areas'),
                'locked_for_me': contact_data.get('$locked_for_me'),
                'from_university_partner': contact_data.get('From_University_partner'),
                'placement_urgency': contact_data.get('Placement_Urgency'),
                'enrich_status': contact_data.get('Enrich_Status__s'),
                'cohort_start_date': parse_datetime_field(contact_data.get('Cohort_Start_Date')),
                'is_duplicate': contact_data.get('$is_duplicate'),
                'signed_agreement': contact_data.get('Signed_Agreement'),
                'accommodation_finalised': contact_data.get('Accommodation_finalised'),
                'send_mail2': contact_data.get('Send_Mail2'),
                't_c_link': contact_data.get('T_C_Link'),
                'number_of_days': contact_data.get('Number_of_Days'),
                'agreement_finalised': contact_data.get('Agreement_finalised'),
                'end_date_auto_populated': parse_datetime_field(contact_data.get('End_date_Auto_populated')),
                'total': contact_data.get('Total'),
                'visa_owner': extract_nested_name(contact_data.get('Visa_Owner')),
                'house_rules': contact_data.get('House_rules'),
                'other_payment_status': contact_data.get('Other_Payment_Status'),
                'days_since_conversion': contact_data.get('Days_Since_Conversion'),
                'name1': contact_data.get('Name1'),
                'average_no_of_days': contact_data.get('Average_no_of_days'),
                'placement_lead_time_days': contact_data.get('Placement_Lead_Time_days'),
                'placement_deadline': parse_datetime_field(contact_data.get('Placement_Deadline')),
                'change_log_time': parse_datetime_field(contact_data.get('Change_Log_Time__s')),
                'community_owner': extract_nested_name(contact_data.get('Community_Owner')),
                'created_by_email': contact_data.get('Created_By', {}).get('email') if isinstance(contact_data.get('Created_By'), dict) else None,
                'decision_date': parse_datetime_field(contact_data.get('Decision_Date')),
                'last_enriched_time': parse_datetime_field(contact_data.get('Last_Enriched_Time__s')),
                'refund_date': parse_datetime_field(contact_data.get('Refund_date')),
                'ps_assigned_date': parse_datetime_field(contact_data.get('PS_Assigned_Date')),
                'books_cust_id': contact_data.get('books_cust_id'),
                'days_count': contact_data.get('Days_Count'),
                'record_status': contact_data.get('Record_Status__s'),
                'type': contact_data.get('Type'),
                'cancellation_notes': contact_data.get('Cancellation_Notes'),
                'locked': contact_data.get('Locked__s'),
                'tag': contact_data.get('Tag'),
                'additional_information': contact_data.get('Additional_Information'),
                'token': contact_data.get('Token'),
                'partnership_specialist_id': extract_nested_name(contact_data.get('Partnership_Specialist')) if contact_data.get('Partnership_Specialist') else None,
            }
            
            # Clean None values and convert boolean fields
            cleaned_fields = {}
            for key, value in contact_fields_mapped.items():
                if value is not None:
                    # Handle boolean fields
                    if key in ['process_flow', 'do_not_contact', 'email_opt_out', 'in_merge', 'locked_for_me', 'is_duplicate', 'send_mail2', 'locked']:
                        cleaned_fields[key] = bool(value) if value in [True, False, 'true', 'false', 1, 0] else False
                    else:
                        cleaned_fields[key] = value
            
            # Update or create contact record
            contact, created = Contact.objects.update_or_create(
                id=contact_id,
                defaults=cleaned_fields
            )
            
            action = "created" if created else "updated"
            logger.info(f"Successfully {action} contact {contact_id} in database")
            
            return {
                'status': 'success',
                'contact_id': contact_id,
                'action': action,
                'message': f'Contact {contact_id} successfully {action} in database'
            }
            
    except Exception as e:
        logger.error(f"Error syncing contact {contact_id}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            'status': 'error',
            'contact_id': contact_id,
            'message': f'Error syncing contact: {str(e)}'
        }
    
@csrf_exempt
@require_http_methods(["POST"])
def contact_sync_webhook(request):
    """Handle Zoho contact sync webhook notifications and sync contact data"""
    try:
        logger.info(f"Step 1. *********Webhook contact sync trigger received *********")

        # Parse request body based on content type
        webhook_data = None
        if request.content_type.startswith('application/x-www-form-urlencoded'):
            # Parse form-encoded data from Zoho
            from urllib.parse import parse_qs, unquote
            raw_data = request.body.decode('utf-8')
            parsed_data = parse_qs(raw_data)
            
            # Convert to single values and decode URL encoding
            webhook_data = {}
            for key, values in parsed_data.items():
                webhook_data[key] = unquote(values[0]) if values else ''
            
            logger.info(f"Step 2. *********Parsed form data received *********")
        elif request.content_type.startswith('application/json'):
            # Parse JSON data
            webhook_data = json.loads(request.body.decode('utf-8'))
            logger.info(f"Step 2. *********Parsed JSON data received *********")
        else:
            logger.error(f"Unsupported content type: {request.content_type}")
            return JsonResponse({'error': 'Unsupported content type'}, status=400)
        
        logger.info(f"Step 3. *********Parsed webhook data received *********")
        logger.info(f"data: {json.dumps(webhook_data, indent=2)[:500]}...")
        
        # Extract contact ID from webhook data
        contact_id = webhook_data.get('id')
        if not contact_id:
            logger.error("No contact ID found in webhook data")
            return JsonResponse({'error': 'No contact ID found in webhook data'}, status=400)
        
        logger.info(f"Step 4. *********Starting contact sync for ID: {contact_id} *********")
        
        # Sync the contact record
        result = sync_single_contact(contact_id)
        
        if result['status'] == 'success':
            logger.info(f"Step 5. *********Contact sync completed successfully for {contact_id} *********")
            return JsonResponse(result)
        else:
            logger.error(f"Contact sync failed for {contact_id}: {result['message']}")
            return JsonResponse(result, status=500)
            
    except Exception as e:
        logger.error(f"Contact sync webhook handling error: {e}")
        logger.error(f"Raw body: {request.body.decode('utf-8')}")
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def health_check(request):
    """Health check endpoint"""
    return JsonResponse({
        'status': 'healthy',
        'service': 'zoho-job-automation',
        'version': '1.0.0'
    })




@csrf_exempt
@require_http_methods(["POST"])
def trigger_job_matching(request, contact_id):
    """Trigger job matching for a specific contact"""
    try:
        result = match_jobs_for_contact(contact_id)
        return JsonResponse(result)
        
    except Exception as e:
        logger.error(f"Job matching trigger error: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def get_job_matches(request, contact_id):
    """Get job matches for a specific contact"""
    try:
        limit = int(request.GET.get('limit', 10))
        
        matches = JobMatch.objects.filter(
            contact_id=contact_id,
            status='active'
        ).order_by('-match_score')[:limit]
        
        matches_data = []
        for match in matches:
            matches_data.append({
                'intern_role_id': match.intern_role_id,
                'match_score': float(match.match_score),
                'industry_match': match.industry_match,
                'location_match': match.location_match,
                'work_policy_match': match.work_policy_match,
                'skill_match': match.skill_match,
                # 'matched_industries': json.loads(match.matched_industries or '[]'),
                'matched_skills': json.loads(match.matched_skills or '[]'),
                'match_reason': match.match_reason,
                'created_at': match.created_at.isoformat()
            })
        
        return JsonResponse({
            'contact_id': contact_id,
            'matches': matches_data,
            'count': len(matches_data)
        })
        
    except Exception as e:
        logger.error(f"Get job matches error: {e}")
        return JsonResponse({'error': str(e)}, status=500)



@csrf_exempt
@require_http_methods(["POST"])
def manual_cv_extraction(request, contact_id):
    """Manually trigger CV extraction and job matching for a contact"""
    try:
        # Get contact info
        try:
            contact = Contact.objects.get(id=contact_id)
            contact_name = contact.full_name or contact.email or 'Unknown'
        except Contact.DoesNotExist:
            return JsonResponse({'error': f'Contact {contact_id} not found'}, status=404)
        
        handler = get_webhook_handler()
        
        # Process CV files
        downloaded_files = handler.process_cv_files(contact_id, contact_name)
        
        # Extract skills
        skills_extracted = handler.extract_skills_from_cvs(contact_id, downloaded_files)
        
        # Match jobs
        match_result = match_jobs_for_contact(contact_id)
        
        return JsonResponse({
            'status': 'success',
            'contact_id': contact_id,
            'contact_name': contact_name,
            'cv_files_processed': len(downloaded_files),
            'skills_extracted': skills_extracted,
            'job_matches': match_result.get('matches_created', 0),
            'match_details': match_result
        })
        
    except Exception as e:
        logger.error(f"Manual CV extraction error: {e}")
@require_http_methods(["GET"])
def get_contact_skills(request, contact_id):
    """Get extracted skills for a specific contact"""
    try:
        skills = Skill.objects.filter(contact_id=contact_id).order_by('-created_at')
        
        skills_data = []
        for skill in skills:
            skills_data.append({
                'skill_name': skill.skill_name,
                'skill_category': skill.skill_category,
                'proficiency_level': skill.proficiency_level,
                'confidence_score': float(skill.confidence_score) if skill.confidence_score else None,
                'extraction_method': skill.extraction_method,
                'created_at': skill.created_at.isoformat()
            })
        
        return JsonResponse({
            'contact_id': contact_id,
            'skills': skills_data,
            'count': len(skills_data)
        })
        
    except Exception as e:
        logger.error(f"Get contact skills error: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@require_http_methods(["POST", "GET"])
def trigger_etl_sync(request):
    """Trigger ETL sync process via API endpoint"""
    try:
        # Get parameters from request
        full_sync = request.GET.get('full', 'false').lower() == 'true'
        entity_type = request.GET.get('entity', 'all')  # all, contacts, accounts, intern_roles
        
        # Determine sync mode (clearer logic)
        incremental_mode = not full_sync  # If full_sync=True, then incremental=False
        sync_mode_description = "FULL" if full_sync else "INCREMENTAL"
        
        # Track start time
        from django.utils import timezone
        start_time = timezone.now()
        
        logger.info(f"ETL sync triggered via API - Entity: {entity_type}, Mode: {sync_mode_description} (incremental={incremental_mode})")
        
        results = {
            'status': 'success',
            'start_time': start_time.isoformat(),
            'entity_type': entity_type,
            'full_sync': full_sync,
            'incremental_mode': incremental_mode,
            'sync_mode': sync_mode_description,
            'results': {}
        }
        
        try:
            if entity_type == 'all':
                # Sync all entities
                logger.info(f"Starting {sync_mode_description} ETL sync for all entities...")
                
                # Sync contacts
                logger.info(f"Syncing contacts ({sync_mode_description})...")
                sync_contacts(incremental=incremental_mode)
                results['results']['contacts'] = 'completed'
                
                # Sync accounts  
                logger.info(f"Syncing accounts ({sync_mode_description})...")
                sync_accounts(incremental=incremental_mode)
                results['results']['accounts'] = 'completed'
                
                # Sync intern roles
                logger.info(f"Syncing intern roles ({sync_mode_description})...")
                sync_intern_roles(incremental=incremental_mode)
                results['results']['intern_roles'] = 'completed'
                
            elif entity_type == 'contacts':
                logger.info(f"Syncing contacts only ({sync_mode_description})...")
                sync_contacts(incremental=incremental_mode)
                results['results']['contacts'] = 'completed'
                
            elif entity_type == 'accounts':
                logger.info(f"Syncing accounts only ({sync_mode_description})...")
                sync_accounts(incremental=incremental_mode)
                results['results']['accounts'] = 'completed'
                
            elif entity_type == 'intern_roles':
                logger.info(f"Syncing intern roles only ({sync_mode_description})...")
                sync_intern_roles(incremental=incremental_mode)
                results['results']['intern_roles'] = 'completed'
                
            else:
                return JsonResponse({
                    'status': 'error',
                    'message': 'Invalid entity type. Use: all, contacts, accounts, or intern_roles'
                }, status=400)
            
            # Calculate duration
            end_time = timezone.now()
            duration = end_time - start_time
            
            results['end_time'] = end_time.isoformat()
            results['duration'] = str(duration)
            results['message'] = f"{sync_mode_description} ETL sync completed successfully in {duration}"
            
            logger.info(f"ETL sync completed successfully - Duration: {duration}")
            
            return JsonResponse(results, status=200)
            
        except Exception as sync_error:
            logger.error(f"ETL sync failed: {sync_error}")
            return JsonResponse({
                'status': 'error',
                'message': f'ETL sync failed: {str(sync_error)}',
                'entity_type': entity_type,
                'full_sync': full_sync
            }, status=500)
            
    except Exception as e:
        logger.error(f"ETL trigger error: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])  
def etl_status(request):
    """Get current ETL sync status and statistics"""
    try:
        from zoho_app.models import SyncTracker, Contact, Account, InternRole
        
        # Get sync tracker information
        sync_trackers = SyncTracker.objects.all()
        trackers_data = []
        
        for tracker in sync_trackers:
            trackers_data.append({
                'entity_type': tracker.entity_type,
                'last_sync_timestamp': tracker.last_sync_timestamp.isoformat() if tracker.last_sync_timestamp else None,
                'records_synced': tracker.records_synced,
                'created_at': tracker.created_at.isoformat(),
                'updated_at': tracker.updated_at.isoformat()
            })
        
        # Get current data counts
        stats = {
            'contacts_count': Contact.objects.count(),
            'accounts_count': Account.objects.count(), 
            'intern_roles_count': InternRole.objects.count(),
            'sync_trackers': trackers_data
        }
        
        return JsonResponse({
            'status': 'success',
            'statistics': stats,
            'message': 'ETL status retrieved successfully'
        })
        
    except Exception as e:
        logger.error(f"ETL status error: {e}")
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@require_http_methods(["POST"])
def trigger_comprehensive_sync(request):
    """
    Trigger comprehensive sync to keep all local data in sync with Zoho
    This endpoint ensures all contacts, accounts, and intern roles are up-to-date
    """
    try:
        # Get parameters from request
        sync_type = request.GET.get('type', 'incremental')  # incremental or full
        entities = request.GET.get('entities', 'all')  # all, contacts, accounts, intern_roles
        
        # Parse request body for specific IDs if provided
        specific_ids = None
        if request.content_type == 'application/json' and request.body:
            data = json.loads(request.body.decode('utf-8'))
            specific_ids = data.get('ids', None)  # List of specific IDs to sync
        
        # Track start time
        start_time = timezone.now()
        
        logger.info(f"=== COMPREHENSIVE SYNC STARTED ===")
        logger.info(f"Sync Type: {sync_type}")
        logger.info(f"Entities: {entities}")
        logger.info(f"Specific IDs: {specific_ids}")
        
        results = {
            'status': 'success',
            'sync_type': sync_type,
            'entities': entities,
            'start_time': start_time.isoformat(),
            'results': {},
            'sync_summary': {}
        }
        
        handler = get_webhook_handler()
        
        # Sync specific entities or all
        if entities in ['all', 'contacts']:
            logger.info("Syncing contacts...")
            if specific_ids and 'contact_ids' in specific_ids:
                # Sync specific contacts
                contact_results = handler.sync_specific_contacts(specific_ids['contact_ids'])
                results['results']['contacts'] = contact_results
            else:
                # Sync all contacts
                incremental = sync_type == 'incremental'
                sync_contacts(incremental=incremental)
                results['results']['contacts'] = 'completed'
        
        if entities in ['all', 'accounts']:
            logger.info("Syncing accounts...")
            if specific_ids and 'account_ids' in specific_ids:
                # Sync specific accounts
                account_results = handler.sync_specific_accounts(specific_ids['account_ids'])
                results['results']['accounts'] = account_results
            else:
                # Sync all accounts
                incremental = sync_type == 'incremental'
                sync_accounts(incremental=incremental)
                results['results']['accounts'] = 'completed'
        
        if entities in ['all', 'intern_roles']:
            logger.info("Syncing intern roles...")
            incremental = sync_type == 'incremental'
            sync_intern_roles(incremental=incremental)
            results['results']['intern_roles'] = 'completed'
        
        # Calculate duration and provide summary
        end_time = timezone.now()
        duration = end_time - start_time
        
        results['end_time'] = end_time.isoformat()
        results['duration'] = str(duration)
        results['message'] = f"Comprehensive sync completed in {duration}"
        
        # Get current data counts for summary
        from zoho_app.models import Contact, Account, InternRole
        results['sync_summary'] = {
            'total_contacts': Contact.objects.count(),
            'total_accounts': Account.objects.count(),
            'total_intern_roles': InternRole.objects.count(),
            'sync_completed_at': end_time.isoformat()
        }
        
        logger.info(f"=== COMPREHENSIVE SYNC COMPLETED ===")
        logger.info(f"Duration: {duration}")
        logger.info(f"Summary: {results['sync_summary']}")
        
        return JsonResponse(results, status=200)
        
    except Exception as e:
        logger.error(f"Comprehensive sync error: {e}")
        return JsonResponse({'error': str(e)}, status=500)

