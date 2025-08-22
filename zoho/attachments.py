#!/usr/bin/env python3
"""
Zoho CRM Attachments Module
Handles fetching and downloading contact attachments (CVs, resumes, etc.)
"""
import os
import re
import logging
import threading
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from urllib.parse import urlparse
from django.db import transaction

from .api_client import ZohoClient
from .auth import get_access_token
from zoho_app.models import Document

# Import skill extractor
try:
    from .skill_extractor import SkillExtractor
    SKILL_EXTRACTION_AVAILABLE = True
    logger = logging.getLogger(__name__)
    logger.info("Skill extractor loaded successfully")
except ImportError as e:
    SKILL_EXTRACTION_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning(f"Skill extractor not available: {e}")


class ZohoAttachmentManager:
    """Manages downloading and organizing contact attachments from Zoho CRM"""
    
    def __init__(self, download_dir: str = "downloads"):
        """
        Initialize the attachment manager
        
        Args:
            download_dir: Directory to store downloaded files
        """
        self.zoho_client = ZohoClient()
        self.download_dir = os.path.normpath(download_dir)
        self.ensure_download_directory()
        
        # Initialize skill extractor
        if SKILL_EXTRACTION_AVAILABLE:
            try:
                self.skill_extractor = SkillExtractor()
                logger.info("Skill extractor initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize skill extractor: {e}")
                self.skill_extractor = None
        else:
            self.skill_extractor = None
        
        # Patterns to identify CV/Resume files
        self.cv_patterns = [
            r'.*cv.*\.pdf$',
            r'.*resume.*\.pdf$', 
            r'.*curriculum.*vitae.*\.pdf$',
            r'.*bio.*\.pdf$',
            r'.*profile.*\.pdf$',
            r'.*portfolio.*\.pdf$'
        ]
    
    def ensure_download_directory(self):
        """Create download directory if it doesn't exist"""
        try:
            os.makedirs(self.download_dir, exist_ok=True)
            logger.info(f"Download directory ready: {self.download_dir}")
        except Exception as e:
            logger.error(f"Failed to create download directory {self.download_dir}: {e}")
            raise
    
    def get_contact_attachments(self, contact_id: str) -> List[Dict]:
        """
        Get all attachments for a specific contact
        
        Args:
            contact_id: Zoho contact ID
            
        Returns:
            List of attachment dictionaries
        """
        try:
            url = f"https://www.zohoapis.com/crm/v2/Contacts/{contact_id}/Attachments"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_access_token()}",
                "Content-Type": "application/json"
            }
            
            import requests
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            attachments = data.get('data', [])
            
            logger.info(f"Found {len(attachments)} attachments for contact {contact_id}")
            return attachments
                
        except Exception as e:
            logger.error(f"Error fetching attachments for contact {contact_id}: {e}")
            return []
    
    def is_cv_file(self, filename: str) -> bool:
        """
        Check if a filename matches CV/resume patterns
        
        Args:
            filename: Name of the file
            
        Returns:
            True if filename appears to be a CV/resume
        """
        if not filename:
            return False
            
        filename_lower = filename.lower()
        
        # Check against CV patterns
        for pattern in self.cv_patterns:
            if re.match(pattern, filename_lower):
                return True
        
        # Additional checks for common CV indicators
        cv_keywords = ['cv', 'resume', 'curriculum', 'vitae', 'bio', 'profile', 'portfolio']
        for keyword in cv_keywords:
            if keyword in filename_lower and filename_lower.endswith('.pdf'):
                return True
        
        return False
    
    def filter_cv_attachments(self, attachments: List[Dict]) -> List[Dict]:
        """
        Filter attachments to find CV/resume files
        
        Args:
            attachments: List of attachment dictionaries
            
        Returns:
            List of CV/resume attachments
        """
        cv_attachments = []
        
        for attachment in attachments:
            filename = attachment.get('File_Name', '')
            if self.is_cv_file(filename):
                cv_attachments.append(attachment)
                logger.info(f"Identified CV file: {filename}")
        
        return cv_attachments
    
    def download_attachment(self, contact_id: str, attachment_id: str, filename: str, 
                          contact_name: str = None, attachment_data: Dict = None) -> Optional[str]:
        """
        Download a specific attachment and save mapping to database
        
        Args:
            contact_id: Zoho contact ID
            attachment_id: Zoho attachment ID
            filename: Name of the file
            contact_name: Name of the contact (for organizing files)
            attachment_data: Full attachment data from Zoho API
            
        Returns:
            Path to downloaded file or None if failed
        """
        try:
            # Create safe filename
            safe_filename = self.create_safe_filename(filename, contact_name, contact_id)
            file_path = os.path.join(self.download_dir, safe_filename)
            
            # Download the file
            download_url = f"https://www.zohoapis.com/crm/v2/Contacts/{contact_id}/Attachments/{attachment_id}"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_access_token()}",
            }
            
            import requests
            response = requests.get(download_url, headers=headers)
            response.raise_for_status()
            
            # Save the file
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            file_size = len(response.content)
            logger.info(f"Downloaded attachment: {safe_filename} ({file_size} bytes)")
            
            # Save document mapping to database
            document_type = self.determine_document_type(filename)
            db_document_id = self.save_document_mapping(
                contact_id, attachment_id, filename, file_path, file_size, attachment_data
            )
            
            # Trigger skill extraction in background if it's a CV and extractor is available
            if self.skill_extractor and self.is_cv_file(filename) and db_document_id:
                self._extract_skills_async(file_path, contact_id, db_document_id, filename)
            
            return file_path
                
        except Exception as e:
            logger.error(f"Error downloading attachment {attachment_id}: {e}")
            return None
    
    def save_document_mapping(self, contact_id: str, document_id: str, document_name: str, 
                            file_path: str, file_size: int, attachment_data: Dict = None) -> Optional[int]:
        """
        Save document mapping to database with duplicate handling
        
        Args:
            contact_id: Zoho contact ID
            document_id: Zoho document/attachment ID
            document_name: Name of the document
            file_path: Local file path
            file_size: Size of the file in bytes
            attachment_data: Full attachment data from Zoho
            
        Returns:
            Database document ID or None if failed
        """
        try:
            with transaction.atomic():
                # Parse Zoho timestamps if available
                zoho_created_time = None
                zoho_modified_time = None
                
                if attachment_data:
                    created_time_str = attachment_data.get('Created_Time')
                    modified_time_str = attachment_data.get('Modified_Time')
                    
                    if created_time_str:
                        try:
                            zoho_created_time = datetime.fromisoformat(created_time_str.replace('Z', '+00:00'))
                        except ValueError:
                            pass
                    
                    if modified_time_str:
                        try:
                            zoho_modified_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))
                        except ValueError:
                            pass
                
                # Check if document already exists
                try:
                    existing_document = Document.objects.get(
                        contact_id=contact_id,
                        document_id=document_id
                    )
                    
                    # Delete old file if it exists and is different from new file
                    old_file_path = existing_document.file_path
                    if old_file_path and old_file_path != file_path and os.path.exists(old_file_path):
                        try:
                            os.remove(old_file_path)
                            logger.info(f"Deleted old file: {old_file_path}")
                        except Exception as e:
                            logger.warning(f"Could not delete old file {old_file_path}: {e}")
                    
                    # Update existing document
                    existing_document.document_name = document_name
                    existing_document.document_type = self.determine_document_type(document_name)
                    existing_document.file_path = file_path
                    existing_document.file_size = file_size
                    existing_document.zoho_created_time = zoho_created_time
                    existing_document.zoho_modified_time = zoho_modified_time
                    existing_document.save()
                    
                    logger.info(f"Updated document record: {document_name} (ID: {existing_document.id})")
                    return existing_document.id
                    
                except Document.DoesNotExist:
                    # Create new document record
                    document = Document.objects.create(
                        contact_id=contact_id,
                        document_id=document_id,
                        document_name=document_name,
                        document_type=self.determine_document_type(document_name),
                        file_path=file_path,
                        file_size=file_size,
                        zoho_created_time=zoho_created_time,
                        zoho_modified_time=zoho_modified_time,
                    )
                    
                    logger.info(f"Created new document record: {document_name} (ID: {document.id})")
                    return document.id
                
        except Exception as e:
            logger.error(f"Error saving document mapping: {e}")
            return None
    
    def determine_document_type(self, filename: str) -> str:
        """
        Determine document type based on filename
        
        Args:
            filename: Name of the file
            
        Returns:
            Document type string
        """
        if not filename:
            return 'unknown'
        
        filename_lower = filename.lower()
        
        if self.is_cv_file(filename):
            return 'cv'
        elif any(keyword in filename_lower for keyword in ['cover', 'letter']):
            return 'cover_letter'
        elif any(keyword in filename_lower for keyword in ['transcript', 'grade']):
            return 'transcript'
        elif any(keyword in filename_lower for keyword in ['certificate', 'cert']):
            return 'certificate'
        elif filename_lower.endswith('.pdf'):
            return 'document'
        elif filename_lower.endswith(('.jpg', '.jpeg', '.png', '.gif')):
            return 'image'
        elif filename_lower.endswith(('.doc', '.docx')):
            return 'word_document'
        else:
            return 'other'
    
    def create_safe_filename(self, original_filename: str, contact_name: str = None, contact_id: str = None) -> str:
        """
        Create a safe filename for local storage
        
        Args:
            original_filename: Original filename from Zoho
            contact_name: Name of the contact
            contact_id: ID of the contact
            
        Returns:
            Safe filename for local storage
        """
        if not original_filename:
            original_filename = 'unknown_file'
        
        # Clean the filename
        safe_filename = re.sub(r'[<>:"/\\|?*]', '_', original_filename)
        safe_filename = safe_filename.strip()
        
        # Add contact prefix if available
        prefix_parts = []
        if contact_name:
            # Clean contact name
            clean_name = re.sub(r'[<>:"/\\|?*]', '_', contact_name)
            clean_name = clean_name.strip()[:50]
            prefix_parts.append(clean_name)
        
        if contact_id:
            prefix_parts.append(contact_id[:10])
        
        if prefix_parts:
            prefix = '_'.join(prefix_parts)
            safe_filename = f"{prefix}_{safe_filename}"
        
        # Ensure filename is not too long
        if len(safe_filename) > 200:
            name, ext = os.path.splitext(safe_filename)
            safe_filename = name[:200-len(ext)] + ext
        
        return safe_filename
    
    def _extract_skills_async(self, file_path: str, contact_id: str, db_document_id: int, filename: str):
        """
        Extract skills from CV in background thread
        
        Args:
            file_path: Path to the downloaded file
            contact_id: Zoho contact ID
            db_document_id: Database document ID
            filename: Original filename
        """
        def extract_skills():
            try:
                logger.info(f"Starting background skill extraction for {filename}")
                skill_ids = self.skill_extractor.extract_and_save_skills(
                    file_path, contact_id, db_document_id
                )
                logger.info(f"Background skill extraction completed: {len(skill_ids)} skills extracted")
            except Exception as e:
                logger.error(f"Background skill extraction failed: {e}")
        
        # Start extraction in background thread
        thread = threading.Thread(target=extract_skills, daemon=True)
        thread.start()
    
    def download_contact_cvs(self, contact_id: str, contact_name: str = None) -> List[str]:
        """
        Download all CV files for a specific contact
        
        Args:
            contact_id: Zoho contact ID
            contact_name: Name of the contact (optional)
            
        Returns:
            List of downloaded file paths
        """
        logger.info(f"Downloading CVs for contact {contact_id}")
        
        # Get all attachments
        attachments = self.get_contact_attachments(contact_id)
        if not attachments:
            logger.info(f"No attachments found for contact {contact_id}")
            return []
        
        # Filter CV attachments
        cv_attachments = self.filter_cv_attachments(attachments)
        if not cv_attachments:
            logger.info(f"No CV attachments found for contact {contact_id}")
            return []
        
        downloaded_files = []
        
        for attachment in cv_attachments:
            attachment_id = attachment.get('id')
            filename = attachment.get('File_Name', 'unknown_file')
            
            if not attachment_id:
                logger.warning(f"No attachment ID found for {filename}")
                continue
            
            file_path = self.download_attachment(
                contact_id, attachment_id, filename, contact_name, attachment
            )
            
            if file_path:
                downloaded_files.append(file_path)
        
        logger.info(f"Downloaded {len(downloaded_files)} CV files for contact {contact_id}")
        return downloaded_files
    
    def get_attachment_info(self, attachment: Dict) -> Dict:
        """
        Extract useful information from attachment data
        
        Args:
            attachment: Attachment data from Zoho API
            
        Returns:
            Dictionary with formatted attachment info
        """
        return {
            'id': attachment.get('id'),
            'filename': attachment.get('File_Name'),
            'size': attachment.get('Size'),
            'created_time': attachment.get('Created_Time'),
            'modified_time': attachment.get('Modified_Time'),
            'created_by': attachment.get('Created_By', {}).get('name'),
            'is_cv': self.is_cv_file(attachment.get('File_Name', ''))
        }
