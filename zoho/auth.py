import requests
import os
from dotenv import load_dotenv

load_dotenv()

def get_access_token():
    """
    Get access token for Zoho CRM API using refresh token
    """
    url = os.getenv("ZOHO_TOKEN_URL")
    payload = {
        'refresh_token': os.getenv("ZOHO_REFRESH_TOKEN"),
        'client_id': os.getenv("ZOHO_CLIENT_ID"),
        'client_secret': os.getenv("ZOHO_CLIENT_SECRET"),
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()['access_token']
