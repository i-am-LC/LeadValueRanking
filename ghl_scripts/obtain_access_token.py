import requests
import os
import time
import json
from dotenv import load_dotenv


def initialise_ghl_tokens():

    # Load the environment variables from the .env file at the start
    load_dotenv()
    ghl_client_id = os.getenv('GHL_CLIENT_ID')
    ghl_client_secret = os.getenv('GHL_CLIENT_SECRET')
    ghl_auth_token = os.getenv('GHL_AUTH_TOKEN')

    access_token = None
    refresh_token = None
    access_token_expiry = 0


    # File to store tokens
    TOKEN_FILE = os.path.join(os.path.dirname(__file__), "ghl-tokens.json")

    # Load tokens from file
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)
            access_token = tokens.get("access_token")
            refresh_token = tokens.get("refresh_token")
            access_token_expiry = tokens.get("access_token_expiry")

    # Function to refresh access token
    def refresh_access_token():
        
        url = "https://services.leadconnectorhq.com/oauth/token"
        data = {
            "client_id": ghl_client_id,
            "client_secret": ghl_client_secret,
            "grant_type": "refresh_token",
            "code": ghl_auth_token,
            "refresh_token": refresh_token,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }

        response = requests.post(url, data=data, headers=headers)
        response_data = response.json()
        access_token = response_data.get('access_token')
        print(access_token)
        access_token_expiry = time.time() + response_data.get('expires_in')
        print("Access token refreshed.")
        save_tokens(access_token, access_token_expiry)   # Save tokens after refresh

        return access_token, refresh_token 

    # Function to save tokens to file
    def save_tokens(access_token, access_token_expiry):
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
        
        # Check if access token is expired
        current_time = time.time()  
        
        if current_time >= float(access_token_expiry):
            print("Access token expired. Refreshing...")
            refresh_access_token()
        else:
            print("Access token is still valid.")

    # Call the function to ensure access token is initialized
    ensure_access_token()

    # Return access token for use in requests
    return access_token
