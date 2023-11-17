# cvm_tld_marx
## TLD CRM Data supplement with Marx Scraped Data.

### This Python script automates the process of updating data in the TLD-CRM system based on information obtained from the CMS portal. It uses Selenium for web scraping and interaction with the CMS portal, fetches data from an input CSV file, performs validations, and updates records in TLD-CRM. The script also handles Azure Key Vault authentication, retrieves One-Time Passcodes (OTPs) from M365 emails, and sends email notifications upon completion.

#### **[TLD_Tiers_Updated.py:](https://drive.google.com/file/d/17crFL5IsGIfHzQLltWNMmDT0d8lzs8fH/view?usp=sharing 'Detailed Documentation')**
Requires a single argument (1, 2, 3). This will generate a Tiers.CSV file depending upon the chosen input. The generated CSV file will be used as an argument for the MARX script. <br>
Command-line usage:<br>
```
python3 TLD_Tiers_Updated.py <1, 2, 3 based on required output>
```

#### **[MARX.py:](https://drive.google.com/file/d/1FTx7U3N90J4XHZR1JuzAJ9c5UlXbFLOV/view?usp=sharing 'Detailed Documentation')**
Requires a single argument containing the name of the CSV generated through the TLD_Tiers script.<br>
Command-line usage:<br>
```
python3 MARX.py <CSV file name i.e Tier1_Policies.csv>
```

#### **contract_directory.xlsx:**
Contains relevant data to find and match Contract number and retrieve Carrier Name and Plan Type.

#### Some important steps to take care of:
**contract_directory.xlsx** file MUST always be in the same directory as the MARX.py script. It is crucial for finding the Carrier Name and Plan Type.

DO NOT execute multiple instances of MARX.py at the same time. This could cause confusion with fetching the 2FA code from the email (as multiple mails would be sent out for each instance).
Recommended time between each run is: 5 minutes.  Look for and replace any ids, keys, and passwords required for your own instance.
