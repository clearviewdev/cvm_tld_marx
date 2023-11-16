# cvm_tld_marx
TLD CRM Data supplement with Marx Scraped Data


TLD_Tiers_Updated.py:
	Requires a single argument (1, 2, 3). This will generate a Tiers.CSV file depending upon the chosen input. The generated CSV file will be used as an argument for the MARX script.
	Command-line usage:
	<path to Python> TLD_Tiers_Updated.py <1, 2, 3 based on required output>


MARX.py:
	Requires a single argument containing the name of the CSV generated through the TLD_Tiers script.
	Command-line usage:
	<path to Python> MARX.py <CSV file name i.e Tier1_Policies.csv>


Some important steps to take care of:
	contract_directory.xlsx file MUST always be in the same directory as the MARX.py script. It is crucial for finding the Carrier Name and Plan Type.

	DO NOT execute multiple instances of MARX.py at the same time. This could cause confusion with fetching the 2FA code from the email (as multiple mails would be sent out for each instance).
	Recommended time between each run is: 5 minutes.  Look for and replace any ids, keys, and passwords required for your own instance.
