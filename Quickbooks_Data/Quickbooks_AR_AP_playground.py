import json
import os
import requests
import pandas as pd

from Quickbooks_Data.intuitlib.client import AuthClient
from Quickbooks_Data.intuitlib.enums import Scopes
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from Quickbooks_Data.Quickbooks_Helper import overall_recursion_pl

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def configure():
    load_dotenv()


client_id = os.getenv("quickbooks_Magnum_Bikes_Analytics_client_id")
client_secret = os.getenv("quickbooks_Magnum_Bikes_Analytics_client_secret")
redirect_uri = "https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl"
environment = "production"
realm_id = "9130354462410786"  # keep as string

token_file = "qb_tokens.json"
auth_client = AuthClient(client_id, client_secret, redirect_uri, environment)

def save_tokens(access_token, refresh_token):
    with open(token_file, 'w') as f:
        json.dump({
            "access_token": access_token,
            "refresh_token": refresh_token
        }, f)

def load_tokens():
    if os.path.exists(token_file):
        with open(token_file, 'r') as f:
            tokens = json.load(f)
            return tokens["access_token"], tokens["refresh_token"]
    return None, None

def refresh_access_token(refresh_token):
    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    response = requests.post(
        token_url,
        headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        },
        auth=HTTPBasicAuth(client_id, client_secret)
    )
    tokens = response.json()
    new_access_token = tokens["access_token"]
    new_refresh_token = tokens.get("refresh_token", refresh_token)  # Sometimes it changes
    save_tokens(new_access_token, new_refresh_token)
    return new_access_token

# STEP 1: Run this part ONLY ONCE to get your initial access/refresh tokens.
# You’ll need to paste in the authorization code from the browser.
def first_time_auth():
    print("Go to the following URL and authorize the app:")
    print(auth_client.get_authorization_url([Scopes.ACCOUNTING])) #link to where get auth code
    auth_code = input("Paste the authorization code here: ")
    auth_client.get_bearer_token(auth_code, realm_id=realm_id)
    save_tokens(auth_client.access_token, auth_client.refresh_token) #save tokens to JSON file
    print("Tokens saved!")

# STEP 2: Use this function in production to always get a valid access token.
# Every time your app runs, it tries to use the refresh token to get a new access token. This way, you don't have to keep logging in manually.
def get_valid_access_token():
    access_token, refresh_token = load_tokens()
    if access_token and refresh_token:
        try:
            # Optionally test if token is still valid — otherwise just refresh it every time
            return refresh_access_token(refresh_token)
        except Exception as e:
            print("Token refresh failed:", e)
            print("You may need to re-authorize.")
            raise
    else:
        print("No tokens found. Run first_time_auth() once to initialize.")
        raise Exception("Missing tokens")






def get_ARAgingDetail(access_token, start_date="2025-04-18", end_date="2025-05-17"):
    base_url = 'https://quickbooks.api.intuit.com'

    url = (
        f"{base_url}/v3/company/{realm_id}/reports/AgedReceivableDetail"
        f"?report_date={start_date}&minorversion=75"
    )

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("✅ Success: ARAgingDetail report fetched.")
        return response.json()
    else:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
        return None

# def clean_ARAgingDetail(start_date):


# MAIN EXECUTION
def main():
    configure()
    # first_time_auth() #Uncomment the line below only once to initialize your tokens

    access_token = get_valid_access_token()


    start_date = "2025-10-14"
    # end_date = "2025-06-24"

    ar_aging_detail = get_ARAgingDetail(access_token, start_date)

    ar_dict_list = ar_aging_detail['Rows']['Row']
    ar_finished_data = overall_recursion_pl(ar_dict_list)

    col_names_ar = ['Date', 'Transaction Type', 'Dont_Know1', 'Num', 'Customer Full Name', 'Dont_Know2', 'Location Full Name',
                    'Dont_Know3 Location ID?', 'Due Date', "Amount",
                    'Open Balance', 'Category']
    df = pd.DataFrame(ar_finished_data, columns=col_names_ar)


    #CLEANING

    # # Split on ":", expand into two columns
    # df[['Customer_Category', 'Customer_Name']] = df['Customer Full Name'].str.split(':', n=1, expand=True)
    # # Fill missing categories with default value
    # df['Customer_Category'] = df['Customer_Category'].fillna("No Customer Category")

    # Split only where ":" exists
    split_cols = df['Customer Full Name'].str.split(':', n=1, expand=True)
    # Assign columns with conditional logic
    df['Customer_Category'] = split_cols[0]
    df['Customer_Name'] = split_cols[1]
    # If no ":" was present, Customer_Name is NaN → move the value into Customer_Name
    mask = df['Customer_Name'].isna()
    df.loc[mask, 'Customer_Name'] = df.loc[mask, 'Customer_Category']
    df.loc[mask, 'Customer_Category'] = "No Customer Category"





    # Define output file path
    output_path = r"C:\Users\joshu\Documents\Reporting\AR_PowerBI\test_AR.xlsx"

    # Use ExcelWriter to write multiple sheets
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(
            writer,
            sheet_name="Test_AR",
            index=False
        )

main()
