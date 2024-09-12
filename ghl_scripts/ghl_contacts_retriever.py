import requests
import os
import pprint
import json

from dotenv import load_dotenv

from .obtain_access_token import initialise_ghl_tokens

load_dotenv()

def clean_contact_data(contact):
    """
    Clean up the contact data and return a dictionary with the desired fields.
    
    Args:
        contact (dict): The contact data to be cleaned up.
    
    Returns:
        dict: A dictionary with the cleaned-up contact data.
    """
    
    # Initialize an empty dictionary to store the cleaned-up contact data
    cleaned_contact = {}
    
    # Add the id, contactName, firstNameRaw, lastNameRaw, companyName, email, phone fields
    cleaned_contact["id"] = contact["id"]
    cleaned_contact["contactName"] = contact["contactName"]
    cleaned_contact["firstName"] = contact["firstName"]
    cleaned_contact["lastName"] = contact["lastName"]
    cleaned_contact["companyName"] = contact["companyName"]
    cleaned_contact["email"] = contact["email"]
    cleaned_contact["phone"] = contact["phone"]
    
    # Add the source field
    cleaned_contact["source"] = contact["source"]
    
    # Add the city, state, postalCode, address1 fields if they exist
    if "city" in contact and contact["city"] is not None:
        cleaned_contact["city"] = contact["city"]
    else:
        cleaned_contact["city"] = ""
        
    if "state" in contact and contact["state"] is not None:
        cleaned_contact["state"] = contact["state"]
    else:
        cleaned_contact["state"] = ""
        
    if "postalCode" in contact and contact["postalCode"] is not None:
        cleaned_contact["postalCode"] = contact["postalCode"]
    else:
        cleaned_contact["postalCode"] = ""
        
    if "address1" in contact and contact["address1"] is not None:
        cleaned_contact["address1"] = contact["address1"]
    else:
        cleaned_contact["address1"] = ""
    
    # Add the dateAdded and dateUpdated fields
    cleaned_contact["dateAdded"] = contact["dateAdded"]
    cleaned_contact["dateUpdated"] = contact["dateUpdated"]
    
    # Add the tags field
    if "tags" in contact and len(contact["tags"]) > 0:
        cleaned_contact["tags"] = contact["tags"]
    else:
        cleaned_contact["tags"] = []
        
    # Add the country field
    cleaned_contact["country"] = contact.get("country", None)
    
    # Add the attributions fields
    if "attributions" in contact and len(contact["attributions"]) > 0:
        cleaned_contact["attributions"] = {}
        for attribution in contact["attributions"]:
            medium = attribution.get("medium", None)
            if medium is not None and "utmCampaign" in attribution and "utmMedium" in attribution and "utmContent" in attribution:
            
                cleaned_contact["attributions"][attribution["medium"]] = {
                    "utmCampaign": attribution.get("utmCampaign", None),
                    "utmMedium": attribution.get("utmMedium", None),
                    "utmContent": attribution.get("utmContent", None),
                    "medium": attribution.get("medium", None),
                }
    else:
        cleaned_contact["attributions"] = {"utmCampaign": None, "utmMedium": None, "utmContent": None}
    
    # Add the customFields field
    if "customFields" in contact and len(contact["customFields"]) > 0:
        cleaned_contact["customFields"] = []
        for customField in contact["customFields"]:
            cleaned_contact["customFields"].append({
                "id": customField["id"],
                "value": customField["value"]
            })
    else:
        cleaned_contact["customFields"] = []
    
    return cleaned_contact

def retrieve_contacts():
    """
    Retrieve all contacts from the GHL API.
    
    Saves the contacts to a JSON file named 'ghl-contacts.json'.
    """
    
    # Get GHL B4B Location key from.env file
    GHL_B4B_LOCATION = os.getenv("GHL_B4B_LOCATION")

    access_token = initialise_ghl_tokens()

    url = "https://services.leadconnectorhq.com/contacts/"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Version": "2021-07-28",
        "Accept": "application/json"
    }

    # Initialize variables to keep track of pagination
    start_after_id = None
    start_after_num = None
    all_contacts = []

    while True:
        querystring = {
            "locationId": GHL_B4B_LOCATION,
            "limit": "100",
            "startAfter": start_after_num,
            "startAfterId": start_after_id,
            "query": "",
        }

        print(f'Making request to {url}')
        response = requests.get(url, headers=headers, params=querystring)

        if response.status_code != 200:
            raise Exception(f"Failed to retrieve contacts. Status code: {response.status_code}")

        # Get the contacts and metadata from the response
        contacts = response.json()['contacts']
        metadata = {}
        for key, value in response.json()['meta'].items():
            metadata[key] = value

        # Update start_after_id for next API call
        start_after_id = metadata['startAfterId']
        start_after_num = metadata['startAfter']

        all_contacts.extend(contacts)

        # If there are no more contacts to retrieve, break the loop
        if not contacts or (metadata and 'total' in metadata and metadata['total'] <= len(all_contacts)):
            break
    
    with open('./ghl_scripts/data/raw-ghl-contacts.json', 'w') as f:
        json.dump(all_contacts, f, indent=4)

    with open('./ghl_scripts/data/clean-ghl-contacts.json', 'w') as f:
        json.dump([clean_contact_data(contact) for contact in all_contacts], f, indent=4)

