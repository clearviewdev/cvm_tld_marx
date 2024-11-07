import requests
import json
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import os
import threading
import queue
import time
import logging

#--------------------------------------------------------
# Loading Azure Keyvault Related Variables from .env file
#--------------------------------------------------------
load_dotenv()
client_id = os.environ['AZURE_CLIENT_ID']
client_secret = os.environ['AZURE_CLIENT_SECRET']
tenant_id = os.environ['AZURE_TENANT_ID']
vault_url = os.environ['AZURE_VAULT_URL']

# Method for Azure Credentials Fetching
def azure_authenticate(client_id, client_secret, tenant_id, vault_url):
    # This function takes the variables fetched from the .env file as an argument
    # and returns a 'secret_client' which can then be used to fetch the secrets from Azure Vault
    
    credentials = ClientSecretCredential(client_id = client_id, client_secret = client_secret, tenant_id = tenant_id)
    
    # Secret client with proper role within Azure to access the secrets within the Key Vault
    secret_client = SecretClient(vault_url= vault_url, credential= credentials)
    
    return secret_client

# Initialize the token bucket with the rate limit
class TokenBucket:
    def __init__(self, rate, capacity):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill_time = time.time()

    def _refill(self):
        now = time.time()
        elapsed_time = now - self.last_refill_time
        tokens_to_add = elapsed_time * self.rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill_time = now

    def get_token(self):
        self._refill()
        if not self.tokens >= 1:
            self.tokens -= 1
            return False
        else:
            return True

# Method to send a PUT request
def send_put_request(lead_id, medicare_claim_number):

    # Creating a payload
    payload = f"lead_id={lead_id}&medicare_claim_number={medicare_claim_number}&marx_plan_change_result={None}"
    
    # Making the PUT request
    try:
        response = requests.put(INGRESS_URL, headers=headers, data=payload, timeout=10)
        time.sleep(1)
        # Reporting
        if response.status_code == 200:
            logging.info(f"PUT request for lead_id {lead_id} with medicare claim number {medicare_claim_number} was successful (Status Code 200)")
        else:
            logging.error(f"PUT request for lead_id {lead_id} with medicare claim number {medicare_claim_number} failed with status code: {response.status_code}")
    except requests.RequestException as e:
        logging.error(f"PUT request for lead_id {lead_id} with medicare claim number {medicare_claim_number} failed due to error: {str(e)}")


# Method to send requests using a specific worker ID
def send_requests(worker_id, request_queue):
    while True:
        try:
            # Get a request from the queue
            lead = request_queue.get()
            if lead is None:
                # Exit the thread when the sentinel value is encountered
                break

            # Check if a token is available in the token bucket
            if not token_bucket.get_token():
                # If no token is available, put the request back in the queue
                request_queue.put(lead)
                time.sleep(1)  # Sleep for a short time and try again
                continue

            # Unpack lead details
            lead_id = lead['lead_id']
            
            # Strip the medicare claim number of dashes
            lead_medicare_claim_number = lead['lead_medicare_claim_number'].replace('-', '')

            # Pass both lead_id and lead_medicare_claim_number to send_put_request
            send_put_request(lead_id, lead_medicare_claim_number)
        except Exception as e:
            # Log the error
            logging.error(f"Worker {worker_id}: Error - {str(e)}")

# Rate Limiter for API
RATE_LIMIT = 10

# Authenticate with Azure Keyvault and retrieve a secret_client after proper handshake
secret_client = azure_authenticate(client_id, client_secret, tenant_id, vault_url)

# Endpoint URLs
EGRESS_URL = "https://cm.tldcrm.com/api/egress/policies"
INGRESS_URL = "https://cm.tldcrm.com/api/ingress/leads"

# List for extracted leads
leads = []

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)

# Format yesterday's date in American format (MM/DD/YYYY)
yesterday_american_format = yesterday.strftime("%m/%d/%Y")

# Parameters for TLD query
params = {
    "columns"  : "policy_id, lead_id, lead_medicare_claim_number, date_sold",
    "limit"    : "0",
    "date_sold" : yesterday_american_format
}

# Headers for API request
headers = {
    'tld-api-id': secret_client.get_secret('tld-api-id').value,
    'tld-api-key': secret_client.get_secret('tld-api-key').value,
    'Cookie': secret_client.get_secret('cookie-value').value
}


while True:
    # Making the EGRESS request to extract policies sold yesterday
    response = requests.get(EGRESS_URL, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        records = data.get('response', {}).get('results', [])

        if records:
            for record in records:
                leads.append({"lead_id": record['lead_id'], "lead_medicare_claim_number": record['lead_medicare_claim_number']})
            print(f"Filtered records found!")
        else:
            print("No filtered records to write.")

        # Break out of the loop when the condition is met
        break
    time.sleep(30)


if leads:
    # Append 'Content-Type' to the headers for PUT request
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    # Logging configuration
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    # Define the rate limit (requests per second)
    token_bucket = TokenBucket(RATE_LIMIT, RATE_LIMIT)

    # Create a request queue
    request_queue = queue.Queue()

    # Add leads to the request queue
    for lead in leads:
        request_queue.put(lead)

    # Create and start worker threads
    num_threads = 3  # Can be adjusted as per requirement
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=send_requests, args=(i, request_queue))
        thread.start()
        threads.append(thread)

    # Add the sentinel value to the queue for each worker thread
    for _ in range(num_threads):
        request_queue.put(None)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    logging.info("All the records were updated successfully within the TLD-CRM")