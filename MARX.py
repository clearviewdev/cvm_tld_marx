from concurrent.futures import ThreadPoolExecutor
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from io import StringIO
from datetime import datetime, date
import openpyxl
import csv
import json
import requests
from bs4 import BeautifulSoup
import sys
import os
from O365 import Account
from O365.message import Message
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv


# Checking if the parameter for the number of parts is present or not.
if len(sys.argv) != 3:
    print("Usage: python script_name.py <input_csv_file> <thread_count>")
    sys.exit(1)

# Checking if the provided CSV file path exists
csv_file_path = sys.argv[1]
if not os.path.exists(csv_file_path):
    raise SystemExit("Provided CSV File not found. Terminating...")

#-----------------------
# VARIABLES DECLARATIONS
#-----------------------

# Naming the error_log.txt file with today's date
error_log_name = f"error_log_{datetime.now().strftime('%m_%d_%Y')}.txt"
send_email = False
policies_count = 0
alerts_count = 0
max_retries = 3
csv_file_path = sys.argv[1]

# Extract the file name from the path
csv_file_name = os.path.basename(csv_file_path)
num_parts = int(sys.argv[2])
current_date = datetime.now().strftime("%m/%d/%Y")

# File and Counter locks
policy_count_lock = threading.Lock()
alerts_count_lock = threading.Lock()
marx_file_lock = threading.Lock()
error_file_lock = threading.Lock()
excel_file_lock = threading.Lock()

# Making an output file for the MARx data if it doesn't exist already
if not os.path.exists('MARx_Update.csv'):
    with open('MARx_Update.csv', 'w', newline='', encoding='utf-8') as data_file:
        header_row = ['marx_last_udpate', 'marx_contract', 'marx_pbp', 'marx_plan_code_desc', 'marx_start_date', 'marx_carrier_name', 'marx_plan_type', 'policy_id', 'lead_id', 'date_effective_in_tld', 'date_sold_in_tld']
        writer= csv.writer(data_file)
        writer.writerow(header_row)

               
#--------------------------------------------------------
# Loading Azure Keyvault Related Variables from .env file
# and obtaining a secret_client object for further process
#--------------------------------------------------------
load_dotenv()
client_id = os.environ['AZURE_CLIENT_ID']
client_secret = os.environ['AZURE_CLIENT_SECRET']
tenant_id = os.environ['AZURE_TENANT_ID']
vault_url = os.environ['AZURE_VAULT_URL']


#----------------------
# FUNCTION DECLARATIONS
#----------------------
def azure_authenticate(client_id, client_secret, tenant_id, vault_url):
    # This method takes the variables fetched from the .env file as an argument
    # and returns a 'secret_client' which can then be used to fetch the secrets from Azure Vault
    
    credentials = ClientSecretCredential(client_id = client_id, client_secret = client_secret, tenant_id = tenant_id)
    
    # Secret client with proper role within Azure to access the secrets within the Key Vault
    secret_client = SecretClient(vault_url= vault_url, credential= credentials)
    
    return secret_client

def get_marx_pbp_and_contract(lead_id):
    url = f"https://cm.tldcrm.com/api/egress/leads?columns=marx_contract,marx_pbp,marx_plan_change_result,marx_last_udpate&import=lead_custom_field&lead_id={lead_id}"
    payload = {}
    headers = {
        'tld-api-id': secret_client.get_secret('tld-api-id').value,
        'tld-api-key': secret_client.get_secret('tld-api-key').value,
        'Cookie': secret_client.get_secret('cookie-value').value
    }

    marx_pbp = ""
    marx_contract = ""
    marx_last_udpate = ""
    marx_plan_change_result = ""

    while True:
        response = requests.get(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = json.loads(response.text)
            if 'response' in data and data['response']['results'] == False:
                break

            # Check if 'response' key exists before accessing its subkeys
            if 'response' in data and 'results' in data['response']:
                results = data['response']['results']
                
                # Assign empty strings to variables if 'key' is not present or NULL
                marx_pbp = str(results.get('marx_pbp', ""))  
                marx_contract = str(results.get('marx_contract', ""))
                marx_last_udpate = str(results.get('marx_last_udpate', ""))
                marx_plan_change_result = str(results.get('marx_plan_change_result', ""))
            
            break  # Exit the loop when the response code is 200
        else:
            # Sleep for a second and retry the request
            time.sleep(1)

    return marx_pbp, marx_contract, marx_last_udpate, marx_plan_change_result

def update_marx_data_in_tld(marx_data):
    # This method takes a data-dictionary as an argument and updates the data in TLD-CRM.
    
    lead_id = marx_data["lead_id"]
    marx_last_udpate = marx_data["marx_last_udpate"]
    marx_contract = marx_data["marx_contract"]
    marx_pbp = marx_data["marx_pbp"]
    marx_plan_code_desc = marx_data["marx_plan_code_desc"]
    marx_start_date = marx_data["marx_start_date"]
    marx_carrier_name = marx_data["marx_carrier_name"]
    marx_plan_type = marx_data["marx_plan_type"]
    marx_plan_change_result = marx_data["marx_plan_change_result"]
    
    # Endpoint URL
    url = "https://cm.tldcrm.com/api/ingress/leads"
    
    # Necessary Headers
    headers = {
      'tld-api-id': secret_client.get_secret('tld-api-id').value,
      'tld-api-key': secret_client.get_secret('tld-api-key').value,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': secret_client.get_secret('cookie-value').value
    }
    # Creating a payload
    payload = f'lead_id={lead_id}&marx_last_udpate={marx_last_udpate}&marx_contract={marx_contract}&marx_pbp={marx_pbp}&marx_plan_code_desc={marx_plan_code_desc}&marx_start_date={marx_start_date}&marx_carrier_name={marx_carrier_name}&marx_plan_type={marx_plan_type}&marx_plan_change_result={marx_plan_change_result}'

    while True:
        # Making the PUT request
        response = requests.put(url, headers=headers, data=payload)
        if response.status_code == 200:
            break
        else:
            # Sleep for a second and retry the request
            time.sleep(1)

def update_blank_data_in_tld(marx_data):
    # This method takes a data-dictionary as an argument and updates the marx_last_update in TLD-CRM.
    
    lead_id = marx_data["lead_id"]
    marx_last_udpate = marx_data["marx_last_udpate"]
    
    # Endpoint URL
    url = "https://cm.tldcrm.com/api/ingress/leads"
    
    # Necessary Headers
    headers = {
      'tld-api-id': secret_client.get_secret('tld-api-id').value,
      'tld-api-key': secret_client.get_secret('tld-api-key').value,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': secret_client.get_secret('cookie-value').value
    }
    # Creating a payload
    payload = f'lead_id={lead_id}&marx_last_udpate={marx_last_udpate}'

    while True:
        # Making the PUT request
        response = requests.put(url, headers=headers, data=payload)
        if response.status_code == 200:
            break
        else:
            # Sleep for a second and retry the request
            time.sleep(1)

def get_OTP(mailbox_secret):
    # This method returns the OTP or 2FA code from the mailbox of the relevant email.
   
    client_id = secret_client.get_secret('client-id').value
    client_secret = secret_client.get_secret('client-secret').value
    tenant_id = secret_client.get_secret('tenant-id').value

    credentials = (client_id, client_secret)

    # Account authentication
    account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id)
    if account.authenticate():
        mailbox = account.mailbox(secret_client.get_secret(mailbox_secret).value)
        query = mailbox.new_query().on_attribute('from').equals('no-reply@idm.cms.gov').order_by(ascending=False)
        query = query.chain().on_attribute('subject').contains('Action Required: One-time verification code')
        
        # Get the latest email with the specified subject
        #messages = mailbox.get_messages(limit=1, query=query)
        message = next(mailbox.get_messages(limit=1, query=query), None)
        if message:
            # Access the body content of the email
            body = message.body
            # Pass body content to HTML parser
            soup = BeautifulSoup(body, 'html.parser')
            # Find the element with the verification code
            verification_code_element = soup.find("span", {"id": "verification-code"})
            # If element is found, return verification code
            if verification_code_element:
                verification_code = verification_code_element.text
                return verification_code
            else:
                raise SystemExit("Email does not contain a Valid Payload. Please run the script again")
        else:
           raise SystemExit("Email does not contain a Valid Payload. Please run the script again")
                
def send_notification(attachment_name):
    # This method sends out a notification email to a pre-defined distribution list about the progress
    
    client_id = secret_client.get_secret('client-id').value
    client_secret = secret_client.get_secret('client-secret').value
    tenant_id = secret_client.get_secret('tenant-id').value
    credentials = (client_id, client_secret)

    # Account authentication
    account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id)
    if account.authenticate():
        mailbox = account.mailbox(secret_client.get_secret('marx-mailbox-email').value) 
        m = mailbox.new_message()
        m.to.add(secret_client.get_secret('agent-alert-email').value)
        m.subject = f"Script Completion Report - {current_date}"
        m.body = f"The MARx script successfully completed the job for {current_date}.<br> CSV File Processed: {csv_file_name}. <br> Total Policies Processed: {policies_count} <br> Total errors that need to be resolved: {alerts_count}"

        # Check if the attachment file exists before adding it
        if os.path.exists(attachment_name):
            m.attachments.add(attachment_name)
        
        # Send notification
        m.send()                

def split_csv_file(csv_file_path, num_parts):
    # This method reads the inputted CSV file and split it into the specified number of parts
    with open(csv_file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)  # First row is the header

        # Get the total number of rows in the CSV file
        total_rows = sum(1 for _ in reader)

        # Calculate the number of rows per part
        rows_per_part = total_rows // num_parts

        # Seek back to the beginning of the file
        csv_file.seek(0)

        # Store all the parts in a list
        csv_parts = []

        # Process each part
        for part_num in range(num_parts):
            start_row = part_num * rows_per_part
            end_row = (part_num + 1) * rows_per_part if part_num < num_parts - 1 else total_rows

            # Seek to the start row
            csv_file.seek(0)  # Move to the beginning of the file
            next(reader)  # Skip the header

            for _ in range(start_row):
                next(reader)

            # Read the rows for the current part
            part = [next(reader) for _ in range(end_row - start_row)]

            # Append the part to the list
            csv_parts.append(part)

    return csv_parts, header

def process_csv_part(part_num, part, header):
    # Function to process partitioned CSV file
    global policies_count
    global alerts_count
    
    #------------------------------------
    # EXECUTING CHROME DRIVER, NAVIGATING
    # AND LOGGING INTO THE CMS PORTAL
    #------------------------------------
    print(f"Executing thread: {part_num}")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    print("Launching Webdriver Instance")
    # Create a WebDriver instance
    driver = webdriver.Chrome(options=chrome_options)

    # Navigate to the URL
    driver.get('https://portal.cms.gov/portal/')


    print("Logging into CMS Portal")
    # Wait for the User ID input field to be visible
    user_id_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-login-userId")))
    user_id = f"cms-portal-id-{part_num}"
    user_id_input.send_keys(secret_client.get_secret(user_id).value)

    # Wait for the Password input field to be visible
    password_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-login-password")))
    password = f"cms-portal-password-{part_num}"
    password_input.send_keys(secret_client.get_secret(password).value)

    # Wait for the Terms and Conditions checkbox to be clickable and click it
    terms_checkbox = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "checkd")))
    driver.execute_script("arguments[0].click();", terms_checkbox)

    time.sleep(5)

    # Wait for the Login button to be clickable and click it
    login_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-login-submit")))
    login_button.click()
    time.sleep(10)

    print("Waiting for the 2FA Code Capture from Outlook")
    # Wait for the MFA Send button to be clickable and click it
    send_mfa_code_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-send-code-phone")))
    send_mfa_code_button.click()
    time.sleep(30)

    #------------------------------
    # FETCHING EMAILS FROM OUTLOOK 
    #------------------------------
    mail_secret = f"cms-mailbox-{part_num}"
    mfa_code = get_OTP(mail_secret)

    if not mfa_code:
        raise SystemExit("Terminate script at this point. No OTP found.")
    #----------------------------
    # BACK TO WEBSITE INTERACTION
    #----------------------------
    mfa_code_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-verify-securityCode")))
    mfa_code_input.send_keys(mfa_code)
    time.sleep(3)

    verify_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-verify-code-submit")))
    verify_button.click()
    time.sleep(30)

    print("Navigating to MARx webpage")
    # Navigate to MARx webpage
    driver.get('https://portal.cms.gov/myportal/wps/myportal/cmsportal/marxaws/verticalRedirect/application')
    time.sleep(5)

    # Wait for the iframe to be visible and switch to it
    iframe = WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.ID, "obj_marxaws_wab_application")))
    driver.switch_to.frame(iframe)

    # Wait for the Logon button to be clickable and click it
    logon_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "userRole")))
    logon_button.click()
    time.sleep(5)

    # Wait for the Beneficiaries button to be clickable and click it
    beneficiaries_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[text()='Beneficiaries ']")))
    beneficiaries_button.click()
    time.sleep(3)

    # Wait for the Eligibility button to be clickable and click it
    eligibility_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[text()='Eligibility ']")))
    eligibility_button.click()
    time.sleep(3)

    print("Starting to input Medicare Numbers into MARx.")

    # Multiple potential errors  in source's HTML
    mbi_error = "<h2>Attention: The beneficiary ID is not a valid MBI number</h2>"
    not_found_error = "<h2>Attention: Beneficiary not found</h2>"
    
    #----------------------------------------
    # AT THE "ELIGIBILITY" PAGE AT THIS POINT
    #----------------------------------------

 
    for row in part:
        with policy_count_lock:
            policies_count += 1
            
        # Get data from Policies CSV
        lead_medicare_claim_number = row[header.index("lead_medicare_claim_number")]
        policy_number = row[header.index("policy_number")]
        date_sold = row[header.index("date_sold")]
        
        # Convert date_sold to a datetime object
        try:
            date_sold_datetime = datetime.strptime(date_sold, "%Y-%m-%d %H:%M:%S").date()
        except ValueError:
            continue
        
        # Only proceed if the medicare_number is 11 digits.
        if len(lead_medicare_claim_number) == 11:
        
            # Show input progress
            print(f"Working on Medicare Number: {lead_medicare_claim_number} | Thread# {part_num}")                                 
                
            # Wait for 60 seconds for the table to load. If it doesn't, refresh and retry until 'max_retries' are exhausted.
            retries = 0
            while retries < max_retries:
                try:
                    # Find and interact with the input_box
                    input_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "claimNumber")))
                    # Clear the input box
                    input_box.clear()
                    time.sleep(1)
                    # Input next medicare number
                    input_box.send_keys(lead_medicare_claim_number)
                    time.sleep(1)
                    # Send "Enter/Return" key as input
                    input_box.send_keys(Keys.RETURN)
                    time.sleep(5)
                    
                    # Checking if the entered MBI Number is valid or not          
                    if mbi_error in driver.page_source:
                        error_message = f"Error: Invalid Medicare Number: {lead_medicare_claim_number} for Policy ID:{row[header.index('policy_id')]}"
                        with error_file_lock:
                            with open(error_log_name, 'a') as error_file:
                                error_file.write(error_message + '\n')
                        break
                    elif not_found_error in driver.page_source:
                        error_message = f"Error: Beneficiary not found for Medicare Number: {lead_medicare_claim_number} for Policy ID:{row[header.index('policy_id')]}"
                        with error_file_lock:
                            with open(error_log_name, 'a') as error_file:
                                error_file.write(error_message + '\n')
                        break
                        
                    # Wait for the results table to load
                    table = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.eligTable7")))
                    break
                except TimeoutException:
                    print("Timeout exception occurred")
                    # Functionality if a Timeout occurs
                    retries += 1
                    
                    # Refresh the page in case a timeout exception occurs and retry
                    driver.get('https://portal.cms.gov/myportal/wps/myportal/cmsportal/marxaws/verticalRedirect/application')
                    time.sleep(10)

                    # Wait for the iframe to be visible and switch to it
                    iframe = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "obj_marxaws_wab_application")))
                    driver.switch_to.frame(iframe)

                    # Wait for the Logon button to be clickable and click it
                    logon_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "userRole")))
                    logon_button.click()
                    time.sleep(6)

                    # Wait for the Beneficiaries button to be clickable and click it
                    beneficiaries_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[text()='Beneficiaries ']")))
                    beneficiaries_button.click()
                    time.sleep(6)

                    # Wait for the Eligibility button to be clickable and click it
                    eligibility_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[text()='Eligibility ']")))
                    eligibility_button.click()
                    time.sleep(6) 
                    
            if (retries == max_retries) or (mbi_error in driver.page_source) or (not_found_error in driver.page_source):
                continue
                
                
            # Extract the table HTML
            table_html = table.get_attribute("outerHTML")

            # Load the HTML table into a Pandas DataFrame
            html_io = StringIO(table_html)

            # Load the HTML table into a Pandas DataFrame
            df = pd.read_html(html_io)[0]

            # Close the StringIO object
            html_io.close()

            # Extract the data from the first row of the DataFrame
            first_row_data = df.iloc[0].tolist()
            
            # Getting today's date
            today = date.today()
            # Format the date in MM/DD/YYYY format
            american_date_format = today.strftime("%m/%d/%Y")
            american_date = datetime.strptime(american_date_format, "%m/%d/%Y").date()
            
            # Checking if customer is enrolled in any plan
            # If any anomalies are encountered, skip to next Medicare number
            try:
                if "The beneficiary is not currently enrolled in any plan" in first_row_data[0].strip():
                    # If customers is not enrolled in any plan, upload blank data to the TLD with today's 'marx_last_udpate' field.
                    blank_data = {
                        "lead_id" : row[header.index('lead_id')],
                        "marx_last_udpate" : american_date_format
                    }
                    update_blank_data_in_tld(blank_data)
                    continue
            except:
                continue
            # Getting marx data:
            marx_last_udpate = american_date_format
            marx_contract = str(first_row_data[0]).strip()
            
            # Typecast PBP to int (originally float)
            try:
                marx_pbp = str(int(first_row_data[1])).strip()
            except:
                marx_pbp = str(first_row_data[1]).strip()
                
            marx_plan_code_desc = str(first_row_data[2]).strip()
            marx_start_date = str(first_row_data[3]).strip()
            marx_carrier_name = ''
            marx_plan_type = ''
            policy_id = row[header.index('policy_id')]
            lead_id = row[header.index('lead_id')]
            date_effective_in_tld = row[header.index('date_effective')]
            date_sold_in_tld = row[header.index('date_sold')]
            
            # Calling API to get the current values of marx_pbp, marx_contract and comparing them with new ones
            marx_plan_change_result = None
            
            # Retrieving old data from API before update
            old_pbp, old_contract, old_last_update, old_plan_result = get_marx_pbp_and_contract(lead_id)
            
            # Calculate the date delta
            date_delta = american_date - date_sold_datetime
            #---------------------------
            # ALERT STATUS FUNCTIONALITY
            #---------------------------
            # If Policy Number is blank, Nothing to compare!
            if policy_number is None or policy_number == '':
                marx_plan_change_result = None
            
            # Proceed if Policy Number is NOT blank
            else:
                # If marx_contract exists in policy_number, we have a 'Match'.
                if marx_contract in policy_number:
                    marx_plan_change_result = 'match'
                
                # If we previously had a 'match' and the updated contract doesn't 'match', trigger an 'Alert'.
                elif old_plan_result == 'match' and marx_contract not in policy_number:
                    with alerts_count_lock:
                        alerts_count+=1
                    marx_plan_change_result = 'Alert'
                
                # If a policy is on Resolved, Retained or Alert, keep as it is!
                elif old_plan_result in ['Resolved', 'Retained', 'Alert']:
                    marx_plan_change_result = old_plan_result
                
                # If it's been 3 or more days since the sale date and the policies still don't match, trigger an 'Alert'
                elif old_plan_result is None and marx_plan_change_result is None and old_last_update is not None and date_delta.days >= 14:
                    with alerts_count_lock:
                        alerts_count+=1
                    marx_plan_change_result = 'Alert'
                
                # No conditionals match, revert policy to None
                else:
                    marx_plan_change_result = None
                
            #-----------------------------------------
            #  Load the Excel file to get
            # 'marx_carrier_name' and 'marx_plan_type'
            #-----------------------------------------
            with excel_file_lock:
                workbook = openpyxl.load_workbook('contract_directory.xlsx')

                # Assuming we are working with the first worksheet in the Excel file
                worksheet = workbook.active

                # Iterate through the rows to find a match in the first column
                for row in worksheet.iter_rows(values_only=True):
                    if row[0] == marx_contract:
                        # Assuming the match is found in the first column (column A)
                        # Get the values from the second and third columns (columns B and C)
                        marx_carrier_name = row[1]
                        marx_plan_type = row[2]  
                        break  # Exit the loop after the first match

                # Close the Excel file
                workbook.close()

            # Creating dictionary for marx_data to be passed as an argument to POST/PUT function.
            marx_data = {
                "lead_id" : lead_id,
                "marx_last_udpate" : marx_last_udpate,
                "marx_contract" : marx_contract,
                "marx_pbp" : marx_pbp,
                "marx_plan_code_desc" : marx_plan_code_desc,
                "marx_start_date" : marx_start_date,
                "marx_carrier_name" : marx_carrier_name,
                "marx_plan_type" : marx_plan_type,
                "marx_plan_change_result" : marx_plan_change_result
            }
            
            #------------------------------------------
            # API CALL TO UPDATE TLD-CRM WITH MARX DATA
            #------------------------------------------
            update_marx_data_in_tld(marx_data)

            # Save data to CSV
            with marx_file_lock:
                with open('MARx_Update.csv', 'a', newline='', encoding='utf-8') as data_file:
                    writer= csv.writer(data_file)
                    writer.writerow([marx_last_udpate, marx_contract, marx_pbp, marx_plan_code_desc, marx_start_date, marx_carrier_name, marx_plan_type, policy_id, lead_id, date_effective_in_tld, date_sold_in_tld])
         
        else:
            # Log error into error file.
            error_message = f"Error: Incorrect Medicare Number: {lead_medicare_claim_number} for Policy ID:{row[header.index('policy_id')]}"
            with error_file_lock:
                with open(error_log_name, 'a') as error_file:
                    error_file.write(error_message + '\n')
    
    # Close the driver when execution is successful
    driver.quit()

def thread_function(part_num):  
    # Function to be executed by each thread
    process_csv_part(part_num, csv_parts[part_num - 1], header)

# Usage of ThreadPoolExecutor
if __name__ == "__main__":

    # Authenticate with Azure Keyvault and retrieve a secret_client after proper handshake        
    secret_client = azure_authenticate(client_id, client_secret, tenant_id, vault_url)
    
    # Split the CSV file into parts
    csv_parts, header = split_csv_file(csv_file_path, num_parts)
    
    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=num_parts) as executor:
        # Schedule the thread_function for each part
        futures = [executor.submit(thread_function, part_num) for part_num in range(1, num_parts + 1)]

        # Wait for all threads to complete
        for future in futures:
            future.result()
        
        # Check if all threads have completed successfully
        all_threads_successful = all(future.done() and future.exception() is None for future in futures)

        # Set send_email to True if all threads were successful
        send_email = all_threads_successful
        
    #----------------------------------
    # SEND EMAIL NOTIFICATION TO AGENTS
    # UPON SUCCESSFUL EXECUTION
    #----------------------------------
    
    if send_email:
        # Send out notification email if all the threads executed successfully.
        send_notification(error_log_name)
