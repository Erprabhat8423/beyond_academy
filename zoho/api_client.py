import requests
from .auth import get_access_token


class ZohoClient:
    """
    Zoho CRM API client for fetching data from different modules
    """
    
    def __init__(self):
        self.base_url = "https://www.zohoapis.com/crm/v2"
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
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json().get('data', [])
            if not data:
                break
            all_data.extend(data)
            if not response.json().get('info', {}).get('more_records'):
                break
            params["page"] += 1

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
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            data = response.json().get('data', [])
            if data and len(data) > 0:
                return data[0]
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching contact {contact_id}: {e}")
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
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json().get('data', [])
        except Exception as e:
            print(f"Error fetching attachments for {record_id}: {e}")
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
            response = requests.get(attachment_url, headers=self.headers)
            response.raise_for_status()
            return response.content
        except Exception as e:
            print(f"Error downloading attachment: {e}")
            return None
