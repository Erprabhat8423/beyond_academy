import requests
import time
import logging
from .auth import get_access_token

logger = logging.getLogger(__name__)


class ZohoClient:
    """
    Zoho CRM API client for fetching data from different modules
    """
    
    def __init__(self, timeout=60, max_retries=3):
        self.base_url = "https://www.zohoapis.com/crm/v2"
        self.timeout = timeout  # Connection and read timeout in seconds
        self.max_retries = max_retries
        
        # Create a session for connection pooling and better performance
        self.session = requests.Session()
        
        self.headers = {
            "Authorization": f"Zoho-oauthtoken {get_access_token()}",
            "Content-Type": "application/json"
        }

    def get_paginated_data(self, module, fields, criteria=None, sort_order=None, sort_by=None):
        """
        Get paginated data from Zoho CRM module
        
        Args:
            module: CRM module name (Contacts, Accounts, etc.)
            fields: List of fields to fetch
            criteria: Filter criteria for API call
            sort_order: Sort order (asc/desc)
            sort_by: Field to sort by
            
        Returns:
            List of records
        """
        url = f"{self.base_url}/{module}"
        params = {
            "fields": ",".join(fields),
            "page": 1,
            "per_page": 200
        }
        
        # Add criteria for filtering (e.g., Modified_Time greater than timestamp)
        if criteria:
            params["criteria"] = criteria
            
        # Add sorting parameters
        if sort_order and sort_by:
            params["sort_order"] = sort_order
            params["sort_by"] = sort_by
            
        all_data = []

        while True:
            # Retry logic for network issues
            for attempt in range(self.max_retries):
                try:
                    # logger.info(f"Fetching {module} data - Page {params['page']}, Attempt {attempt + 1}")
                    response = self.session.get(
                        url, 
                        headers=self.headers, 
                        params=params, 
                        timeout=self.timeout
                    )
                    response.raise_for_status()
                    
                    data = response.json().get('data', [])
                    if not data:
                        logger.info(f"No more data found for {module} on page {params['page']}")
                        return all_data
                        
                    all_data.extend(data)
                    logger.info(f"Retrieved {len(data)} records from {module} (page {params['page']}, total so far: {len(all_data)})")
                    
                    if not response.json().get('info', {}).get('more_records'):
                        logger.info(f"Completed fetching {module} data - Total records: {len(all_data)}")
                        return all_data
                        
                    params["page"] += 1
                    break  # Success, exit retry loop
                    
                except requests.exceptions.Timeout as e:
                    logger.warning(f"Timeout on attempt {attempt + 1} for {module} page {params['page']}: {e}")
                    if attempt == self.max_retries - 1:
                        logger.error(f"Max retries exceeded for {module} page {params['page']}")
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
                except requests.exceptions.ConnectionError as e:
                    logger.warning(f"Connection error on attempt {attempt + 1} for {module} page {params['page']}: {e}")
                    if attempt == self.max_retries - 1:
                        logger.error(f"Max retries exceeded for {module} page {params['page']}")
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Request error for {module} page {params['page']}: {e}")
                    raise

        return all_data

    def get_contact_by_id(self, contact_id):
        """
        Get a single contact by ID from Zoho CRM
        
        Args:
            contact_id: The ID of the contact to fetch
            
        Returns:
            Contact data dictionary or None if not found
        """
        try:
            url = f"{self.base_url}/Contacts/{contact_id}"
            response = self.session.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json().get('data', [])
            if data and len(data) > 0:
                return data[0]
            return None
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching contact {contact_id}: {e}")
            return None

    def get_attachments(self, module, record_id):
        """
        Get attachments for a specific record
        
        Args:
            module: CRM module name
            record_id: Record ID
            
        Returns:
            List of attachment data
        """
        try:
            url = f"{self.base_url}/{module}/{record_id}/actions/download_photo"
            response = self.session.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get('data', [])
        except Exception as e:
            logger.error(f"Error fetching attachments for {record_id}: {e}")
            return []

    def get_related_records(self, module, record_id, related_module, fields=None):
        """
        Get related records for a specific record (e.g., Deals for an Account)
        
        Args:
            module: Primary module name (e.g., 'Accounts')
            record_id: ID of the primary record
            related_module: Related module name (e.g., 'Deals')
            fields: List of fields to fetch from related records
            
        Returns:
            List of related records
        """
        try:
            url = f"{self.base_url}/{module}/{record_id}/{related_module}"
            params = {}
            
            if fields:
                params["fields"] = ",".join(fields)
            
            response = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            # Handle empty response
            response_text = response.text.strip()
            if not response_text:
                logger.debug(f"Empty response for {related_module} records for {module} {record_id}")
                return []
            
            try:
                response_data = response.json()
            except ValueError as e:
                logger.warning(f"Invalid JSON response for {related_module} records for {module} {record_id}: {response_text[:100]}")
                return []
            
            data = response_data.get('data', [])
            if data:
                logger.info(f"Retrieved {len(data)} {related_module} records for {module} {record_id}")
            else:
                logger.debug(f"No {related_module} records found for {module} {record_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error fetching {related_module} for {module} {record_id}: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching {related_module} for {module} {record_id}: {e}")
            return []

    def download_attachment(self, attachment_url):
        """
        Download attachment file
        
        Args:
            attachment_url: URL to download attachment from
            
        Returns:
            File content as bytes or None
        """
        try:
            # Use longer timeout for file downloads
            response = self.session.get(attachment_url, headers=self.headers, timeout=self.timeout * 2)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return None
