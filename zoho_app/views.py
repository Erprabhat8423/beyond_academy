import json
import logging
import hmac
import hashlib
import threading
import asyncio
import os
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
            
            # Step 2: Always update local contact data with the most current information
            logger.info(f"Step 6. *********Updating local contact data for {contact_id} *********")
            self.update_local_contact(contact_info)
            
            # Step 3: Sync related account data if contact has account association
            account_id = contact_info.get('Account_Name', {}).get('id') if isinstance(contact_info.get('Account_Name'), dict) else None
            if account_id:
                logger.info(f"Step 9. *********Syncing related account data for account {account_id} *********")
                self.sync_related_account(account_id)
            
            # Step 4: Trigger incremental sync for intern roles to keep job data fresh
            logger.info(f"Step 12. *********Triggering incremental sync for intern roles *********")
            self.sync_intern_roles_incremental()
            
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
        Update account in local database
        
        Args:
            account_info: Account information dictionary
            
        Returns:
            True if update was successful
        """
        try:
            from zoho_app.models import Account
            
            account_id = account_info.get('id')
            if not account_id:
                logger.warning("No account ID provided for local update")
                return False
            
            # Try to find and update existing account
            try:
                account = Account.objects.get(id=account_id)
                logger.info(f"Updating existing account {account_id}")
                
                # Update fields if provided
                if account_info.get('Account_Name'):
                    account.account_name = account_info['Account_Name']
                if account_info.get('Website'):
                    account.website = account_info['Website']
                if account_info.get('Phone'):
                    account.phone = account_info['Phone']
                if account_info.get('Industry'):
                    account.industry = account_info['Industry']
                if account_info.get('Annual_Revenue'):
                    account.annual_revenue = account_info['Annual_Revenue']
                if account_info.get('Employees'):
                    account.employees = account_info['Employees']
                if account_info.get('Billing_Street'):
                    account.billing_address = f"{account_info.get('Billing_Street', '')} {account_info.get('Billing_City', '')} {account_info.get('Billing_State', '')}"
                
                # Update timestamp
                account.updated_time = timezone.now()
                account.save()
                logger.info(f"Successfully updated local account {account_id}")
                
            except Account.DoesNotExist:
                logger.info(f"Account {account_id} not found locally - creating new record")
                
                # Create new account record
                account = Account.objects.create(
                    id=account_id,
                    account_name=account_info.get('Account_Name', ''),
                    website=account_info.get('Website', ''),
                    phone=account_info.get('Phone', ''),
                    industry=account_info.get('Industry', ''),
                    annual_revenue=account_info.get('Annual_Revenue'),
                    employees=account_info.get('Employees'),
                    billing_address=f"{account_info.get('Billing_Street', '')} {account_info.get('Billing_City', '')} {account_info.get('Billing_State', '')}",
                    created_time=timezone.now(),
                    updated_time=timezone.now()
                )
                logger.info(f"Created new local account {account_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating local account: {e}")
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
                'matched_industries': json.loads(match.matched_industries or '[]'),
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

