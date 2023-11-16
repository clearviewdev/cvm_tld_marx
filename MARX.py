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
from datetime import date
from datetime import datetime
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


# Checking if the parameter file is present or not.
if len(sys.argv) != 2:
    print("Usage: python script_name.py <input_csv_file>")
    sys.exit(1)

csv_file_path = sys.argv[1]   

# Extract the file name from the path
csv_file_name = os.path.basename(csv_file_path)



#--------------------------------------------------------
# Loading Azure Keyvault Related Variables from .env file
#--------------------------------------------------------
load_dotenv()
client_id = os.environ['AZURE_CLIENT_ID']
client_secret = os.environ['AZURE_CLIENT_SECRET']
tenant_id = os.environ['AZURE_TENANT_ID']
vault_url = os.environ['AZURE_VAULT_URL']

#-----------------------
# VARIABLES DECLARATIONS
#-----------------------

# Naming the error_log.txt file with today's date
error_log_name = f"error_log_{datetime.now().strftime('%m_%d_%Y')}.txt"
send_email = False
policies_count = 0
alerts_count = 0
max_retries = 2

#----------------------
# FUNCTION DECLARATIONS
#----------------------
def azure_authenticate(client_id, client_secret, tenant_id, vault_url):
    # This function takes the variables fetched from the .env file as an argument
    # and returns a 'secret_client' which can then be used to fetch the secrets from Azure Vault
    
    credentials = ClientSecretCredential(client_id = client_id, client_secret = client_secret, tenant_id = tenant_id)
    
    # Secret client with proper role within Azure to access the secrets within the Key Vault
    secret_client = SecretClient(vault_url= vault_url, credential= credentials)
    
    return secret_client

def get_marx_pbp_and_contract(lead_id):
    # This function takes in lead_id as an argument and returns their marx_pbp and marx_contract information. 
    # This information will be used later on in the code to make the comparison and change the value of marx_plan_change_result field

    url = f"https://cm.tldcrm.com/api/egress/leads?columns=marx_contract,marx_pbp,marx_plan_change_result,marx_last_udpate&import=lead_custom_field&lead_id={lead_id}"
    payload = {}
    headers = {
        'tld-api-id': secret_client.get_secret('tld-api-id').value,
        'tld-api-key': secret_client.get_secret('tld-api-key').value,
        'Cookie': secret_client.get_secret('cookie-value').value
    }

    marx_pbp = ""
    marx_contract = ""

    while True:
        response = requests.get(url, headers=headers, data=payload)
        if response.status_code == 200:
            data = json.loads(response.text)
            if data['response']['results'] == False:
                break
            # Extract the values of relevant data from the API response
            marx_pbp = str(data['response']['results']['marx_pbp'] or "")  # Assign empty string if null
            marx_contract = str(data['response']['results']['marx_contract'] or "")  # Assign empty string if null
            marx_last_udpate = str(data['response']['results']['marx_last_udpate'] or "")
            marx_plan_change_result = str(data['response']['results']['marx_plan_change_result'] or "")
            
            break  # Exit the loop when the response code is 200
        else:
            print("Response code is not 200. Retrying...")

    return marx_pbp, marx_contract, marx_last_udpate, marx_plan_change_result

def update_marx_data_in_tld(marx_data):
    # This function takes a data-dictionary as an argument and updates the data in TLD-CRM.
    
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
            print("Retrying PUT request")

def get_OTP():
    # This function returns the OTP or 2FA code from the mailbox of the relevant email.
   
    client_id = secret_client.get_secret('client-id').value
    client_secret = secret_client.get_secret('client-secret').value
    tenant_id = secret_client.get_secret('tenant-id').value

    credentials = (client_id, client_secret)

    # Account authentication
    account = Account(credentials, auth_flow_type='credentials', tenant_id=tenant_id)
    if account.authenticate():
        mailbox = account.mailbox(secret_client.get_secret('marx-mailbox-email').value)
        query = mailbox.new_query().on_attribute('subject').contains('Action Required: One-time verification code').order_by(ascending=False)
        # Get the latest email with the specified subject
        messages = mailbox.get_messages(limit=1, query=query)
        
        for message in messages:
            if isinstance(message, Message):
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
                    raise SystemExit("Email does not contain a Valid Payload. Terminate script at this point.")
            else:
                raise SystemExit("Email does not contain a Valid Payload. Terminate script at this point.")
                
def send_notification(attachment_name):
    # This function sends out a notification email to a pre-defined distribution list about the progress
    
    client_id = secret_client.get_secret('client-id').value
    client_secret = secret_client.get_secret('client-secret').value
    tenant_id = secret_client.get_secret('tenant-id').value
    
    current_date = datetime.now().strftime("%m/%d/%Y")
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

        m.send()

#------------------------------------
# EXECUTING CHROME DRIVER, NAVIGATING
# AND LOGGING INTO THE CMS PORTAL
#------------------------------------

chrome_options = Options()
chrome_options.add_argument("--headless")
print("Launching Webdriver Instance")
# Create a WebDriver instance
driver = webdriver.Chrome(options=chrome_options)

# Navigate to the URL
driver.get('https://portal.cms.gov/portal/')

# Authenticate with Azure Keyvault and retrieve a secret_client after proper handshake
secret_client = azure_authenticate(client_id, client_secret, tenant_id, vault_url)

print("Logging into CMS Portal")
# Wait for the User ID input field to be visible
user_id_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-login-userId")))
user_id_input.send_keys(secret_client.get_secret('cms-portal-id').value)

# Wait for the Password input field to be visible
password_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cms-login-password")))
password_input.send_keys(secret_client.get_secret('cms-portal-password').value)

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
mfa_code = get_OTP()

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
beneficiaries_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='navsm' and text()='Beneficiaries ']")))
beneficiaries_button.click()
time.sleep(3)

# Wait for the Eligibility button to be clickable and click it
eligibility_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='navsm' and text()='Eligibility']")))
eligibility_button.click()
time.sleep(3)

print("Starting to input Medicare Numbers into MARx.")

#----------------------------------------
# AT THE "ELIGIBILITY" PAGE AT THIS POINT
#----------------------------------------

# Making an output file for the MARx data if it doesn't exist already
if not os.path.exists('MARx_Update.csv'):
    with open('MARx_Update.csv', 'w', newline='', encoding='utf-8') as data_file:
        header_row = ['marx_last_udpate', 'marx_contract', 'marx_pbp', 'marx_plan_code_desc', 'marx_start_date', 'marx_carrier_name', 'marx_plan_type', 'policy_id', 'lead_id', 'date_effective_in_tld', 'date_sold_in_tld']
        writer= csv.writer(data_file)
        writer.writerow(header_row)
    
if csv_file_path:
    # Open the selected CSV file
    with open(csv_file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            policies_count+=1
            lead_medicare_claim_number = row.get("lead_medicare_claim_number")
            
            # Only proceed if the medicare_number is 11 digits.
            if len(lead_medicare_claim_number) == 11:
                # Find and interact with the input_box
                print(f"Working on Medicare Number: {lead_medicare_claim_number}")
                input_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "claimNumber")))
                input_box.clear()
                input_box.send_keys(lead_medicare_claim_number)
                time.sleep(1)
                input_box.send_keys(Keys.RETURN)
                time.sleep(24)
                
                # Checking if the entered MBI Number is valid or not
                mbi_error = "<h2>Attention: The beneficiary ID is not a valid MBI number</h2>"
                not_found_error = "<h2>Attention: Beneficiary not found</h2>"
                if mbi_error in driver.page_source:
                    error_message = f"Error: Invalid Medicare Number: {lead_medicare_claim_number} for Policy ID:{row.get('policy_id')}"
                    with open(error_log_name, 'a') as error_file:
                        error_file.write(error_message + '\n')
                    continue
                elif not_found_error in driver.page_source:
                    error_message = f"Error: Beneficiary not found for Medicare Number: {lead_medicare_claim_number} for Policy ID:{row.get('policy_id')}"
                    with open(error_log_name, 'a') as error_file:
                        error_file.write(error_message + '\n')
                    continue
                    
                # Wait for 60 seconds for the table to load. If it doesn't, refresh and retry until 'max_retries' are exhausted.
                retries = 0
                while retries < max_retries:
                    try:
                        table = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.eligTable7")))
                        break
                    except TimeoutException:
                        print("Timeout exception occurred")
                        # Functionality if the table doesn't appear within the designed time frame
                        retries += 1
                        driver.get('https://portal.cms.gov/myportal/wps/myportal/cmsportal/marxaws/verticalRedirect/application')
                        time.sleep(10)

                        # Wait for the iframe to be visible and switch to it
                        iframe = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "obj_marxaws_wab_application")))
                        driver.switch_to.frame(iframe)

                        # Wait for the Logon button to be clickable and click it
                        logon_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.ID, "userRole")))
                        logon_button.click()
                        time.sleep(10)

                        # Wait for the Beneficiaries button to be clickable and click it
                        beneficiaries_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='navsm' and text()='Beneficiaries ']")))
                        beneficiaries_button.click()
                        time.sleep(10)

                        # Wait for the Eligibility button to be clickable and click it
                        eligibility_button = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='navsm' and text()='Eligibility']")))
                        eligibility_button.click()
                        time.sleep(10)
                        
                        input_box = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "claimNumber")))
                        input_box.clear()
                        input_box.send_keys(lead_medicare_claim_number)
                        input_box.send_keys(Keys.RETURN)
                        time.sleep(25)
                        
                        # Checking if the entered MBI Number is valid or not
                        mbi_error = "<h2>Attention: The beneficiary ID is not a valid MBI number</h2>"
                        not_found_error = "<h2>Attention: Beneficiary not found</h2>"
                        if mbi_error in driver.page_source:
                            error_message = f"Error: Invalid Medicare Number: {lead_medicare_claim_number} for Policy ID:{row.get('policy_id')}"
                            with open(error_log_name, 'a') as error_file:
                                error_file.write(error_message + '\n')
                            break
                        elif not_found_error in driver.page_source:
                            error_message = f"Error: Beneficiary not found for Medicare Number: {lead_medicare_claim_number} for Policy ID:{row.get('policy_id')}"
                            with open(error_log_name, 'a') as error_file:
                                error_file.write(error_message + '\n')
                            break
                        
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
                
                # Checking if customer is enrolled in any plan
                if "The beneficiary is not currently enrolled in any plan" in first_row_data[0]:
                    # If customers is not enrolled in any plan, upload blank data to the TLD with today's 'marx_last_udpate' field.
                    blank_data = {
                    "lead_id" : row.get("lead_id"),
                    "marx_last_udpate" : american_date_format,
                    "marx_contract" : '',
                    "marx_pbp" : '',
                    "marx_plan_code_desc" : '',
                    "marx_start_date" : '',
                    "marx_carrier_name" : '',
                    "marx_plan_type" : '',
                    "marx_plan_change_result" : ''
                    }
                    update_marx_data_in_tld(blank_data)
                    continue
                        
                # Getting marx data:
                marx_last_udpate = american_date_format
                marx_contract = str(first_row_data[0])
                marx_pbp = str(first_row_data[1])
                marx_plan_code_desc = str(first_row_data[2])
                marx_start_date = str(first_row_data[3])
                marx_carrier_name = ''
                marx_plan_type = ''
                policy_id = row.get("policy_id")
                lead_id = row.get("lead_id")
                date_effective_in_tld = row.get("date_effective")
                date_sold_in_tld = row.get("date_sold")
                
                # Calling API to get the current values of marx_pbp, marx_contract and comparing them with new ones
                marx_plan_change_result = ''
                
                # Retrieving old data from API before update
                old_pbp, old_contract, old_last_update, old_plan_result = get_marx_pbp_and_contract(lead_id)

                #---------------------------
                # ALERT STATUS FUNCTIONALITY
                #---------------------------
                
                # If last_update is empty, no alert is triggered, value inputted for first time.
                if not old_last_update:
                    marx_plan_change_result = ''
                    
                # If alert is already triggered, keep it triggered even if the values match. (Alert raised but not fixed yet)
                elif old_plan_result == 'Alert':
                    marx_plan_change_result = 'Alert'
                    alerts_count+=1
                
                # If the values dont' match, trigger an alert and increase the alert counter
                elif old_pbp and old_contract and (old_pbp != marx_pbp and old_contract != marx_contract):
                    marx_plan_change_result = 'Alert'
                    # Increase the alert counter by 1
                    alerts_count+=1
                
                # If none of the above criteria followed, don't trigger the alert.
                else:
                    marx_plan_change_result = ''
                    
                #-----------------------------------------
                #  Load the Excel file to get
                # 'marx_carrier_name' and 'marx_plan_type'
                #-----------------------------------------

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
                with open('MARx_Update.csv', 'a', newline='', encoding='utf-8') as data_file:
                    writer= csv.writer(data_file)
                    writer.writerow([marx_last_udpate, marx_contract, marx_pbp, marx_plan_code_desc, marx_start_date, marx_carrier_name, marx_plan_type, policy_id, lead_id, date_effective_in_tld, date_sold_in_tld])
             
            else:
                # Log error into error file.
                error_message = f"Error: Incorrect Medicare Number: {lead_medicare_claim_number} for Policy ID:{row.get('policy_id')}"
                with open(error_log_name, 'a') as error_file:
                    error_file.write(error_message + '\n')
                    
    send_email = True
else:
    raise SystemExit("No CSV file selected. Terminate script at this point.")


print("Successfully fetched all the data into output file: MARx_Update.csv and updated it in TLD-CRM.")

#----------------------------------
# SEND EMAIL NOTIFICATION TO AGENTS
#----------------------------------
if send_email == True:               
    send_notification(error_log_name)

# Close the browser window
driver.quit()