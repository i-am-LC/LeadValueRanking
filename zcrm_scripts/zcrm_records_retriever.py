import requests
from pprint import pprint
import json

from .obtain_access_token import initialize_zoho_tokens

# Constants
BASE_URL = 'https://www.zohoapis.com/crm/v6/'
ACCESS_TOKEN = initialize_zoho_tokens()

def make_api_request(url: str, headers: dict) -> dict:
    """
    Makes a GET request to the specified URL with the provided headers.

    Args:
        url (str): The URL to make the request to.
        headers (dict): The headers to include in the request.

    Returns:
        dict: The parsed JSON response.
    """
    print(f'Making request to {url}')
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def get_zcrm_data(endpoint: str, criteria: str) -> dict:
    """
    Makes a request to the Zoho CRM API and returns the parsed JSON response.

    Args:
        endpoint (str): The endpoint to make the request to.
        criteria (str): The criteria to include in the request.

    Returns:
        dict: The parsed JSON response.
    """
    url = f"{BASE_URL}{endpoint}/search?criteria={criteria}"
    headers = {"Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}"}
    return make_api_request(url, headers)


def save_cleaned_zcrm_data(data: dict, filename: str, specified_fields: list) -> None:
    """
    Saves the cleaned Zoho CRM data to a JSON file.

    Args:
        data (dict): The data to clean and save.
        filename (str): The filename to save the data to.
        specified_fields (list): The fields to include in the cleaned data.
    """
    result = [{field: item[field] for field in specified_fields} for item in data['data']]
    with open(filename, 'w') as file:
        json.dump(result, file, indent=4)


def clean_zcrm_leads(data: dict) -> None:
    """
    Cleans the Zoho CRM leads data and saves it to a JSON file.

    Args:
        data (dict): The data to clean and save.
    """
    specified_fields = [
        'Company', 'Contact_type', 'Converted_Account', 'Converted_Contact', 'Converted_Deal',
        'Country', 'Created_Time', 'Deal_Name', 'Deal_Type', 'Email', 'First_Name', 'Full_Name',
        'Generic_Email', 'Industry', 'Last_Name', 'Lead_Number', 'Lead_Source', 'Lead_Status',
        'Lead_source_notes', 'Mobile', 'Phone'
    ]
    save_cleaned_zcrm_data(data, './zcrm_scripts/data/clean-zcrm-leads.json', specified_fields)


def clean_zcrm_deals(data: dict) -> None:
    """
    Cleans the Zoho CRM deals data and saves it to a JSON file.

    Args:
        data (dict): The data to clean and save.
    """
    specified_fields = [
        'Deal_Name', 'Checked_Signed_off', 'Stage', 'Created_Time', 'Agreement_Approved', 'Emergency_Forward_No',
        'Solution_delivered', 'Generic_Email', 'Accepted_by_Provisioning', 'Amount', 'Contact_Name', 
        'Lead_Source', 'SAF_Sent', 'Grand_Total', 'Monthly_Sub_Total', 'Octane_ID', 'Agreement_Returned_On',
        'Deal_Type', 'Proposal_Sent', 'Handsets_Required', 'Lines_Required', 
    ]
    save_cleaned_zcrm_data(data, './zcrm_scripts/data/clean-zcrm-deals.json', specified_fields)


def zcrm_list_leads() -> dict:
    """
    Makes a request to the Zoho CRM API and returns a list of all leads with a Lead Source of 'B4B'.

    Returns:
        dict: The parsed JSON response.
    """
    criteria = "(Lead_Source:equals:B4B)&(Lead_Source:equals:B4B Unqualified)"
    return get_zcrm_data("Leads", criteria)


def zcrm_list_deals() -> dict:
    """
    Makes a request to the Zoho CRM API and returns a list of all deals with a Lead Source of 'B4B'.

    Returns:
        dict: The parsed JSON response.
    """
    criteria = "(Lead_Source:equals:B4B)&(Lead_Source:equals:B4B Unqualified)"
    return get_zcrm_data("Deals", criteria)


def zcrm_get_latest() -> None:
    leads = zcrm_list_leads()
    if leads:
        clean_zcrm_leads(leads)

    deals = zcrm_list_deals()
    if deals:
        clean_zcrm_deals(deals)








    
