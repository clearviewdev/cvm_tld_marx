import argparse
import requests
import json
import csv
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import os


# Create an argument parser
parser = argparse.ArgumentParser(description="Filter and process policies based on selected tier")
parser.add_argument("selected_tier", type=int, choices=[1, 2, 3], help="Select a tier (1, 2, or 3)")

# Parse the command-line arguments
args = parser.parse_args()
selected_tier = args.selected_tier

#--------------------------------------------------------
# Loading Azure Keyvault Related Variables from .env file
#--------------------------------------------------------
load_dotenv()
client_id = os.environ['AZURE_CLIENT_ID']
client_secret = os.environ['AZURE_CLIENT_SECRET']
tenant_id = os.environ['AZURE_TENANT_ID']
vault_url = os.environ['AZURE_VAULT_URL']

def azure_authenticate(client_id, client_secret, tenant_id, vault_url):
    # This function takes the variables fetched from the .env file as an argument
    # and returns a 'secret_client' which can then be used to fetch the secrets from Azure Vault
    
    credentials = ClientSecretCredential(client_id = client_id, client_secret = client_secret, tenant_id = tenant_id)
    
    # Secret client with proper role within Azure to access the secrets within the Key Vault
    secret_client = SecretClient(vault_url= vault_url, credential= credentials)
    
    return secret_client

# Endpoint URL
url = "https://cm.tldcrm.com/api/egress/policies"

# Parameters for the query
params = {
    "columns"  : "policy_id, lead_id, lead_medicare_claim_number, status_description, status_id, date_effective, date_sold",
    "limit"    : "0",
    "status_id": "1"
}

# Authenticate with Azure Keyvault and retrieve a secret_client after proper handshake
secret_client = azure_authenticate(client_id, client_secret, tenant_id, vault_url)

headers = {
    'tld-api-id': secret_client.get_secret('tld-api-id').value,
    'tld-api-key': secret_client.get_secret('tld-api-key').value,
    'Cookie': secret_client.get_secret('cookie-value').value
}

response = requests.get(url, params=params, headers=headers)

if response.status_code == 200:
    data = response.json()
    records = data.get('response', {}).get('results', [])

    # Create a dictionary to track the latest policy_id for each unique lead_medicare_claim_number
    latest_policy_id = {}
    filtered_records = []

    for record in records:
        lead_medicare_claim_number = record.get('lead_medicare_claim_number')
        policy_id = record.get('policy_id')
        date_sold = record.get('date_sold')
        date_effective = record.get('date_effective')

        # Check if lead_medicare_claim_number is None or empty, skip the record
        if not lead_medicare_claim_number:
            continue

        # Check if this record is the latest for the given lead_medicare_claim_number
        if lead_medicare_claim_number in latest_policy_id:
            if policy_id > latest_policy_id[lead_medicare_claim_number]:
                latest_policy_id[lead_medicare_claim_number] = policy_id
        else:
            latest_policy_id[lead_medicare_claim_number] = policy_id

        filtered_records.append(record)

    # Filter the records to keep only the latest policy_id for each unique lead_medicare_claim_number
    filtered_records = [record for record in filtered_records if record['policy_id'] == latest_policy_id[record['lead_medicare_claim_number']]]

    if selected_tier == 1:
        # Filter Tier 1: Date_effective > today's date and Date_sold = 7 days before today
        csv_filename = "Tier1_Policies.csv"
        current_date = datetime.now()
        seven_days_ago = current_date - timedelta(days=7)
        filtered_records = [record for record in filtered_records if
                            record['date_effective'] is not None and
                            datetime.strptime(record['date_effective'], "%Y-%m-%d").date() > current_date.date() and
                            datetime.strptime(record['date_sold'], "%Y-%m-%d %H:%M:%S").date() < seven_days_ago.date()]
                            
    elif selected_tier == 2:
        # Filter Tier 2: Date_effective older than today's date and within the past 90 days
        csv_filename = "Tier2_Policies.csv"
        current_date = datetime.now()
        past_90_days = current_date - timedelta(days=90)
        filtered_records = [record for record in filtered_records if
                            record['date_effective'] is not None and
                            datetime.strptime(record['date_effective'], "%Y-%m-%d").date() < current_date.date() and
                            past_90_days.date() <= datetime.strptime(record['date_effective'], "%Y-%m-%d").date()]
                            
                            
    elif selected_tier == 3:
        # Filter Tier 3: Date_effective older than 90 days
        csv_filename = "Tier3_Policies.csv"
        current_date = datetime.now()
        past_90_days = current_date - timedelta(days=90)
        filtered_records = [record for record in filtered_records if
                            record['date_effective'] is not None and
                            datetime.strptime(record['date_effective'], "%Y-%m-%d").date() < past_90_days.date()]

    if filtered_records:
        # Write the filtered records to a CSV file
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = filtered_records[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for record in filtered_records:
                writer.writerow(record)

        print(f"Filtered records written to {csv_filename}")
    else:
        print("No filtered records to write.")
else:
    print(f"Failed to retrieve data with status code {response.status_code}. Reason: {response.text}")