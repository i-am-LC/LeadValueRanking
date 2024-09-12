# GoHighLevel and Zoho CRM Data Integration Project


## Overview
This project integrates data from GoHighLevel and Zoho CRM APIs to provide detailed insights, ranking and value potential of each lead campaign being run.

## Requirements
See requirements.txt for a list of all required libraries.

## Project Structure
* `zcrm_scripts/`: Module for retrieving data from Zoho CRM API and storing of data in JSON format
* `ghl_scripts/`: Module for retrieving data from GoHighLevel API and storing of data in JSON format
* `main.py`: Main script for retrieving data from both APIs and saving the results in CSV format

## Usage
1. Clone the repository to your local machine.
2. Install required libraries by running `pip install -r requirements.txt`.
3. Configure your API keys and credentials in the `zcrm_scripts` and `ghl_scripts` modules.
4. Run the script using `python main.py`.
5. The script will retrieve data from APIs, clean and transform it, and save the results to `detailed_results.csv` and `condensed_results.csv` files.

## Data Cleaning and Transformation
The script performs the following data cleaning and transformation steps:

* Retrieves latest data from GoHighLevel and Zoho CRM APIs using custom modules.
* Loads data from JSON files into pandas DataFrames.
* Cleans and transforms data using custom functions:
    + `clean_ghl_contacts`: Cleans and transforms GoHighLevel contacts data.
    + `clean_zcrm_leads`: Cleans and transforms Zoho CRM leads data.
    + `clean_zcrm_deals`: Cleans and transforms Zoho CRM deals data.
* Joins data from different sources using `join_data` function.
* Assigns ranking to the data using `assign_ranking` function.

## Results
The script saves the results to two CSV files:

* `detailed_results.csv`: Contains all the data with detailed ranking information.
* `condensed_results.csv`: Contains condensed data with only relevant columns and ranking information.
