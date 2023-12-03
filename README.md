# cvm_tld_marx
## TLD CRM Data supplement with Marx Scraped Data.

### This Python script automates the process of updating data in the TLD-CRM system based on information obtained from the CMS portal. It uses Selenium for web automation and interaction with the CMS portal, fetches data from an input CSV file, performs validations, and updates records in TLD-CRM. The script also handles Azure Key Vault authentication, retrieves One-Time Passcodes (OTPs) from M365 mailboxes, and sends out email notifications upon completion.

#### **[TLD_Tiers_Updated.py:](https://drive.google.com/file/d/17crFL5IsGIfHzQLltWNMmDT0d8lzs8fH/view 'Detailed Documentation')**
Requires a single argument (1, 2 or 3). This will generate a Tiers.CSV file depending upon the chosen input. The generated CSV file will be used as an argument for the MARX script in the next step. <br>
Command-line usage:<br>
```
python3 TLD_Tiers_Updated.py <1, 2, 3 based on required tier>
```

#### **[MARX.py:](https://drive.google.com/file/d/1cD2_oX9T9ai0lBaaGYP_R7U50drn_o8M/view 'Detailed Documentation')**
Requires 2 arguments containing the name of the CSV generated through the TLD_Tiers script as well as the number of accounts to be used.<br>
Command-line usage:<br>
```
python3 MARX.py <CSV file name i.e Tier1_Policies.csv> <Number of threads to launch i.e 2>
```
The above command will utilize 2 CMS accounts to retrieve the data requested in Tier1_Policies.csv
#### **[contract_directory.xlsx:](https://docs.google.com/spreadsheets/d/1RueedxgYvXycOgmRffDHv26vmcbpUE5bPt3PNB-a35w/edit 'Google Spreadsheet')**
Contains relevant data to find and match Contract Number and retrieve Carrier Name and Plan Type.

### **Some important steps to take care of**:
**contract_directory.xlsx** file must __*always*__ be in the same directory as the MARX.py script. It is crucial for finding the Carrier Name and Plan Type.

__*MAKE SURE*__ all the relevant secret variables for multiple CMS accounts are present inside the Azure Key Vault before proceeding.

__*DO NOT*__ execute multiple instances of MARX.py at the same time. This could cause confusion with fetching the 2FA code from the email (as multiple OTPs would be sent out for each instance).
Recommended time between each run is: 3-5 minutes. Look for and replace any IDs, Keys and Passwords required for your own instance.


