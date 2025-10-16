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



def get_pl_report(access_token, start_date="2025-04-18", end_date="2025-05-17"):
    base_url = 'https://quickbooks.api.intuit.com'

    url = (
        f"{base_url}/v3/company/{realm_id}/reports/ProfitAndLossDetail"
        f"?start_date={start_date}&end_date={end_date}&minorversion=75"
    )
    # url = (
    #     f"{base_url}/v3/company/{realm_id}/reports/GeneralLedger"
    #     f"?account={account_id}&start_date={start_date}&end_date={end_date}&minorversion=75"
    # )

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print("✅ Success: Profit and Loss report fetched.")
        return response.json()
    else:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
        return None

def Quickbooks_pl_report_clean(start_date, end_date):
    configure()
    first_time_auth()  # Uncomment the line below only once to initialize your tokens
    access_token = get_valid_access_token()

    pl_data = get_pl_report(access_token, start_date, end_date)

    pl_dict_list = pl_data['Rows']['Row']
    pl_finished_data = overall_recursion_pl(pl_dict_list)

    col_names_pl = ['Date', 'txn_type', 'txn_num', 'Num', 'Name', 'Name_id', 'location', 'location_id', "class",
                    'class_id', 'memo', 'split_acc', 'split_id', 'Amount', 'Balance', 'GL Code']
    df = pd.DataFrame(pl_finished_data, columns=col_names_pl)

    # Add new col with numeric part of GL Code
    df['GL Code Numeric'] = df['GL Code'].str.extract(r'(\d+)')  # Extract digits
    df['GL Code Numeric'] = df['GL Code Numeric'].fillna(0).astype(int)

    # # Convert Date and Make Month/Year cols
    # df['Date'] = pd.to_datetime(df['Date'])
    # df['Month'] = df['Date'].dt.month
    # df['Year'] = df['Date'].dt.year

    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

    return df


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


    start_date = "2024-06-25"
    end_date = "2025-06-24"

    # ar_aging_detail = get_ARAgingDetail(access_token, start_date)
    #
    # print(ar_aging_detail)
    # print("DID IT")

    pl_data = get_pl_report(access_token, start_date, end_date)

    pl_dict_list = pl_data['Rows']['Row']
    pl_finished_data = overall_recursion_pl(pl_dict_list)

    col_names_pl = ['Date', 'txn_type', 'txn_num', 'Num', 'Name', 'Name_id', 'location', 'location_id', "class", 'class_id', 'memo', 'split_acc', 'split_id', 'Amount', 'Balance', 'GL Code']
    df = pd.DataFrame(pl_finished_data, columns=col_names_pl)

    #Add new col with numeric part of GL Code
    df['GL Code Numeric'] = df['GL Code'].str.extract(r'(\d+)')  # Extract digits
    df['GL Code Numeric'] = df['GL Code Numeric'].fillna(0).astype(int)

    #Convert Date and Make Month/Year cols
    df['Date'] = pd.to_datetime(df['Date'])
    df['Month'] = df['Date'].dt.month
    df['Year'] = df['Date'].dt.year

    #convert 'Amount' column to float
    # df['Amount'] = df['Amount'].astype(float)
    df['Amount'] = df['Amount']

    # Format the filename with dates
    file_path_df = fr"C:\Users\joshu\Documents\P&L_Reports\Profit_and_Loss_Report_{start_date}_to_{end_date}.xlsx"

    # Save to Excel
    df.to_excel(
        file_path_df,
        sheet_name="Profit and Loss Report",
        index=True
    )


    #Expense by Account and Vendor report
    other_list_expenses = ['Accounting fees', 'Internet & TV services', 'Liability insurance',
                          'Memberships & subscriptions', 'Officers\' life insurance',
                          'Payroll Fees', 'Phone service', 'Reimbursement Clearing',
                          'Vehicle gas & fuel', 'Workers\' compensation insurance']
    expenses_df = df.loc[((df['GL Code Numeric'] >= 6000) & (df['GL Code Numeric'] < 7000)) |
                         ((df['GL Code Numeric'] >= 70000) & (df['GL Code Numeric'] < 80000)) |
                         ((df['GL Code Numeric'] >= 80000) & (df['GL Code Numeric'] < 90000)) |
                         (df['GL Code'].isin(other_list_expenses))]
    rel_cols_expenses = ['GL Code', 'Name', 'Amount', 'Year', 'Month']
    expenses_df = expenses_df[rel_cols_expenses].reset_index(drop=True)
    expenses_by_account_and_vendor_report = expenses_df.groupby(['GL Code', 'Name', 'Year', 'Month'])['Amount'].sum().round(2).reset_index()


    # Revenue by Customer Report, 4000 level GL Codes
    other_list_revenue = ['Discounts given']
    revenue_df = df.loc[((df['GL Code Numeric'] >= 4000) & (df['GL Code Numeric'] < 5000)) | (df['GL Code'].isin(other_list_revenue))]
    rel_cols = ['Name', 'Amount', 'Year', 'Month']
    revenue_df = revenue_df[rel_cols].reset_index(drop=True)
    # revenue_group = revenue_df.groupby(['Name', 'Year', 'Month']).sum().round(2) #Nicer format, does not repeat information
    revenue_by_customer_report = revenue_df.groupby(['Name', 'Year', 'Month'])['Amount'].sum().round(2).reset_index()  # Nicer format, does not repeat information


    #COGS By Vendor Report
    other_list_cogs = ['Jam-N Storage Fee', 'Storage Rental', 'Warehouse Labor']
    cogs_df = df.loc[((df['GL Code Numeric'] >= 5000) & (df['GL Code Numeric'] < 6000)) | df['GL Code'].isin(other_list_cogs)]
    cogs_df = cogs_df[rel_cols].reset_index(drop=True)
    cogs_by_vendor_report = cogs_df.groupby(['Name', 'Year', 'Month'])['Amount'].sum().round(2).reset_index()


    # Define output file path
    output_path = r"C:\Users\joshu\Documents\P&L_Reports\P&L_Reports.xlsx"

    # Use ExcelWriter to write multiple sheets
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        expenses_by_account_and_vendor_report.to_excel(
            writer,
            sheet_name="Expenses by Customer and Vendor",
            index=True
        )

        revenue_by_customer_report.to_excel(
            writer,
            sheet_name="Revenue by Customer",
            index=True
        )

        cogs_by_vendor_report.to_excel(
            writer,
            sheet_name="Cogs by Vendor",
            index=True
        )

# main()