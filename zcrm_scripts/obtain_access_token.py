import requests
import os
import time
import json
from dotenv import load_dotenv

def initialize_zoho_tokens():
    # Load the environment variables from the .env file at the start
    load_dotenv()

    client_id = os.getenv("ZCRM_CLIENT_ID")
    client_secret = os.getenv("ZCRM_CLIENT_SECRET")
    grant_token = os.getenv("ZCRM_GRANT_TOKEN")

    access_token = None
    refresh_token = None
    access_token_expiry = 0

    # File to store tokens
    TOKEN_FILE = os.path.join(os.path.dirname(__file__), "zcrm-tokens.json")

    # Load tokens from file
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)
            access_token = tokens.get("access_token")
            refresh_token = tokens.get("refresh_token")
            access_token_expiry = tokens.get("access_token_expiry")

    # Function to refresh access token
    def refresh_access_token(refresh_token):
        nonlocal access_token, access_token_expiry
        print("Refreshing access token...")
        url = 'https://accounts.zoho.com/oauth/v2/token'
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token
        }
        response = requests.post(url, data=data)
        response_data = response.json()
        access_token = response_data.get('access_token')
        access_token_expiry = time.time() + response_data.get('expires_in')
        print("Access token refreshed.")
        save_tokens()  # Save tokens after refresh
        return access_token, refresh_token

    # Function to get access token
    def get_access_token(grant_token):
        nonlocal access_token, refresh_token, access_token_expiry
        print("Obtaining access token...")
        url = 'https://accounts.zoho.com/oauth/v2/token'
        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'authorization_code',
            'code': grant_token,
            'redirect_uri': 'https://google.com'
        }
        response = requests.post(url, data=data)
        response_data = response.json()
        
        if 'error' in response_data and response_data['error'] == 'invalid_code':
            print("Error: Invalid grant token")
            return None, None

        access_token = response_data.get('access_token')
        refresh_token = response_data.get('refresh_token')
        expires_in = response_data.get('expires_in')
        if expires_in is None:
            print("Error: 'expires_in' not found in response data")
            return None, None
        access_token_expiry = time.time() + expires_in
        print("Access token obtained.")
        save_tokens()  # Save tokens after obtaining
        return access_token, refresh_token

    # Function to save tokens to file
    def save_tokens():
        with open(TOKEN_FILE, "w") as f:
            tokens = {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "access_token_expiry": access_token_expiry
            }
            json.dump(tokens, f)
        print("Tokens saved.")

    # Function to ensure access token is valid, refreshing if necessary
    def ensure_access_token():
        nonlocal access_token, refresh_token, access_token_expiry
        if not access_token:
            access_token, refresh_token = get_access_token(grant_token)
        else:
            # Check if access token is expired
            current_time = time.time()
            if current_time >= access_token_expiry:
                print("Access token expired. Refreshing...")
                access_token, refresh_token = refresh_access_token(refresh_token)
            else:
                print("Access token is still valid.")
    
    # Call the function to ensure access token is initialized
    ensure_access_token()
    
    # Return the access token
    return access_token