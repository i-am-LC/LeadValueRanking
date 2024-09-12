import json
import pandas as pd

from zcrm_scripts.zcrm_records_retriever import zcrm_get_latest
from ghl_scripts.ghl_contacts_retriever import retrieve_contacts

# Retrieve the latest Zoho CRM and GHL data and save to files.
zcrm = zcrm_get_latest()
ghl = retrieve_contacts()

# Load data from JSON files
def load_data(file_path):
    """Load data from a JSON file"""
    return pd.read_json(file_path)


# Clean GHL contacts data
def clean_ghl_contacts(data):
    """Clean GHL contacts data"""
    data.drop(['id', 'firstName', 'lastName', 'city', 'state', 'postalCode', 'address1', 'dateAdded', 'dateUpdated', 'country'], axis=1, inplace=True)
    
    # Standardize values for matching
    data['email_ghlc'] = data['email'].astype(str).str.lower()
    data['phone_ghlc'] = data['phone'].astype(str).str.lower().str.replace('61', '0').str.replace(' ', '').str.replace('.0', '')
    data['contactName_ghlc'] = data['contactName'].astype(str).str.lower()
    
    # Extract 'attributions' data
    for attribution in data['attributions'].iloc[0].keys():
        for key in data['attributions'].iloc[0][attribution].keys():
            new_column_name = f"{attribution}_{key}"
            data[new_column_name] = data['attributions'].apply(lambda row: row.get(attribution, {}).get(key, None))
    
    # Extract custom fields values
    business_in_au_field_id = "rXRaOb44Zgb853REc5Wo"
    handset_count_field_id = "vq0Esn3nuJ2jknUuvjhU"
    ad_name_field_id = "WY19sqzAA5ApOI573VVl"
    ph_verified_field_id = "zAKDOxzWoIGAX7Nadsqk"
    qualified_field_id = "uV1tzJy3WNtlIw8UIdYP"
    
    def extract_value(row, id):
        custom_fields = row.get('customFields', [])
        for field in custom_fields:
            if field['id'] == id:
                value = field['value']
                if isinstance(value, list):
                    return value[0]
                return value
        return None
    
    data['Business_in_AU'] = data.apply(lambda row: extract_value(row, business_in_au_field_id), axis=1)
    data['Handset_Count'] = data.apply(lambda row: extract_value(row, handset_count_field_id), axis=1)
    data['Ad_Name'] = data.apply(lambda row: extract_value(row, ad_name_field_id), axis=1)
    data['Ph_verified'] = data.apply(lambda row: extract_value(row, ph_verified_field_id), axis=1)
    data['Qualified'] = data.apply(lambda row: extract_value(row, qualified_field_id), axis=1)
    
    data.drop(['attributions', 'customFields'], axis=1, inplace=True)

    # drop any rows that have a source of 'b4b - no txt conf form' or 'B4B Website Survey'
    data = data[~((data['source'] == 'b4b - no txt conf form') | (data['source'] == 'B4B Website Survey') | (data['source'] == 'bestforbusiness'))]
    
    return data


# Clean ZCRM leads
def clean_zcrm_leads(data):
    """Clean ZCRM leads data"""
    
    # Standardize values for matching
    data['email_zl'] = data['Email'].astype(str).str.lower()
    data.drop('Email', axis=1, inplace=True)
    data['phone_zl'] = data['Phone'].astype(str).str.lower().str.replace('+61', '0').str.replace(' ', '')
    data['contactName_zl'] = data['Full_Name'].astype(str).str.lower()

    # Drop unused columns
    data.drop(
        ['Lead_source_notes', 'Converted_Account', 'Converted_Contact', 
               'Converted_Deal', 'Country', 'Created_Time', 'First_Name', 
               'Industry', 'Last_Name', 'Full_Name', 'Phone', 'Contact_type', 
               'Deal_Name', 'Deal_Type', 'Generic_Email', 'Mobile'
        ], 
        axis=1, inplace=True
    )
    
    return data


# Clean ZCRM deal data
def clean_zcrm_deals(data):
    """Clean ZCRM deals data"""
    data.drop(['Checked_Signed_off', 'Created_Time', 'Agreement_Approved', 'Solution_delivered', 'Accepted_by_Provisioning', 'SAF_Sent', 'Agreement_Returned_On', 'Proposal_Sent', 'Lead_Source'], axis=1, inplace=True)
    
    # Standardize values for matching
    data['email_zd'] = data['Generic_Email'].astype(str).str.lower()
    data['phone_zd'] = data['Emergency_Forward_No'].astype(str).str.lower().str.replace('+61', '0').str.replace(' ', '')
    data.drop(['Generic_Email', 'Emergency_Forward_No'], axis=1, inplace=True)
    
    data['contactName_zd'] = data['Contact_Name'].apply(lambda x: x['name']).astype(str).str.lower()
    data.drop('Contact_Name', axis=1, inplace=True)
    
    return data


# Join data
def join_data(ghl_contacts, zcrm_leads, zcrm_deals):
    """Join GHL contacts, ZCRM leads, and ZCRM deals data"""
    result = ghl_contacts.merge(zcrm_leads, how='left', left_on='email_ghlc', right_on='email_zl', suffixes=('_ghlc', '_zl'))
    
    # Match ZCRM deals to GHL contacts
    for index, row in result.iterrows():
        match = False
        for key, value in row[['contactName_ghlc', 'email_ghlc', 'phone_ghlc']].items():
            matching_row = zcrm_deals[zcrm_deals[key.replace('_ghlc', '_zd')] == value]
            if not matching_row.empty:
                match = True
                result.at[index, 'Amount'] = matching_row['Amount'].values[0]
                result.at[index, 'Stage'] = matching_row['Stage'].values[0]
                # Add as many fields as you need
                break
        if not match:
            result.at[index, 'Deal_ID'] = None
            result.at[index, 'Deal_Owner'] = None
                
    return result


def assign_ranking(data: pd.DataFrame) -> pd.DataFrame:
    """Assign ranking to the data"""
    def assign_rank(row):
        if (
            row['tags'] is None or 'phone verified' not in row['tags']
        ) and str(row['Amount']) == 'nan' and (
            row['Ph_verified'] is None and row['Qualified'] is None
        ):
            return (1, 'Spammer')
        else:
            if row['Stage'] == "Deal Timed Out":
                return (0, 'Unknown')
            elif row['Handset_Count'] == "1-2":
                if str(row['Amount']) != 'nan' and row['Stage'] != "Deal Timed Out":
                    return (12, 'Sold and delivered')
                if (row['Ph_verified'] == "True" or row['Qualified'] == "True") and str(row['Amount']) == 'nan':
                    return (7, '1-2 line | responded | no sale')
                if row['Ph_verified'] is None and row['Qualified'] is None:
                    return (2, '1-2 line | no response')                
            elif row['Handset_Count'] == "3-4":
                if str(row['Amount']) != 'nan' and (row['Stage'] == "Checked & Signed Off" and row['Stage'] != "Deal Timed Out"):
                    return (12, 'Sold and delivered')
                if (row['Ph_verified'] == "True" or row['Qualified'] == "True") and str(row['Amount']) == 'nan':
                    return (8, '3-4 line | responded | no sale')
                if row['Ph_verified'] is None and row['Qualified'] is None:
                    return (3, '3-4 line | no response')  
            elif row['Handset_Count'] == "5-9":
                if str(row['Amount']) != 'nan' and row['Stage'] != "Deal Timed Out":
                    return (12, 'Sold and delivered')
                if (row['Ph_verified'] == "True" or row['Qualified'] == "True") and str(row['Amount']) == 'nan':
                    return (9, '5-9 line | responded | no sale')
                if row['Ph_verified'] is None and row['Qualified'] is None:
                    return (4, '5-9 line | no response')   
            elif row['Handset_Count'] == "10-24":
                if str(row['Amount']) != 'nan' and row['Stage'] != "Deal Timed Out":
                    return (12, 'Sold and delivered')
                if (row['Ph_verified'] == "True" or row['Qualified'] == "True") and str(row['Amount']) == 'nan':
                    return (10, '10-24 line | responded | no sale')
                if row['Ph_verified'] is None and row['Qualified'] is None:
                    return (5, '10-24 line | no response')
            elif row['Handset_Count'] == "25+":
                if str(row['Amount']) != 'nan' and row['Stage'] != "Deal Timed Out":
                    return (12, 'Sold and delivered')
                if (row['Ph_verified'] == "True" or row['Qualified'] == "True") and str(row['Amount']) == 'nan':
                    return (11, '25+ line | responded | no sale')
                if row['Ph_verified'] is None and row['Qualified'] is None:
                    return (6, '25+ line | no response')
            else:
                return (0, 'Unknown')
    
    result = data.copy()
    result[['ranking', 'ranking_desc']] = result.apply(assign_rank, axis=1, result_type='expand')
    return result


if __name__ == '__main__':
    # Load data
    ghl_contacts = load_data("./ghl_scripts/data/clean-ghl-contacts.json")
    zcrm_leads = load_data("./zcrm_scripts/data/clean-zcrm-leads.json")
    zcrm_deals = load_data("./zcrm_scripts/data/clean-zcrm-deals.json")
    
    # Clean data
    ghl_contacts_cleaned = clean_ghl_contacts(ghl_contacts)
    zcrm_leads_cleaned = clean_zcrm_leads(zcrm_leads)
    zcrm_deals_cleaned = clean_zcrm_deals(zcrm_deals)
    
    # Join data
    result = join_data(ghl_contacts_cleaned, zcrm_leads_cleaned, zcrm_deals_cleaned)
    
    # Assign ranking
    result = assign_ranking(result)
    
    # Save results to CSV
    result.to_csv('detailed_results.csv', index=False)
    result.drop(
        columns=['tags', 'Company', 'Lead_Number', 'Lead_Source', 'Lead_Status',
                 'phone_zl', 'email_ghlc', 'phone_ghlc', 'contactName_ghlc', 
                 'email_zl', 'contactName_zl', 'Deal_ID', 
                 'Deal_Owner', 
                 ]
    ).to_csv('condensed_results.csv', index=False)