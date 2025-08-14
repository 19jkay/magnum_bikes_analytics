import hmac
import json
import hashlib
import base64
import requests
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

from Unleashed_Data.Unleashed_Helper import convert_ms_date

def configure():
    load_dotenv()


unleashed_api_key = os.getenv("unleashed_Magnum_Bikes_Analytics_api_key")
unleashed_api_id = os.getenv("unleashed_Magnum_Bikes_Analytics_api_id")


# main function to send a get request to retrieve data from unleashed API
def unleashed_api_get_request(url_base, page_number, url_param):
  # Create url
  url_path = url_base + "/" + str(page_number) + '/'
  url = "https://api.unleashedsoftware.com/" + url_path + '?' + url_param
  signature = hmac.new(unleashed_api_key.encode('utf-8'), url_param.encode('utf-8'), hashlib.sha256)
  # Create auth token using url params and api key
  auth_token = signature.digest()
  auth_token64 = base64.b64encode(auth_token)
  # set request headers
  headers = {
    'Accept': 'application/json',
    'api-auth-id': unleashed_api_id,
    'api-auth-signature': auth_token64,
    'Content-Type': 'application/json'
  }
  # perform get request
  unleashed_data = requests.get(url=url, headers=headers)
  # convert json to dict
  unleashed_json = json.loads(unleashed_data.content)
  return unleashed_json









def get_products_helper(unleashed_data_name, url_param="", page_number=1):
    url_base = unleashed_data_name + "/Page"
    return unleashed_api_get_request(url_base, page_number, url_param)

def get_customers_helper(unleashed_data_name, url_param="", page_number=1):
    url_base = unleashed_data_name + "/Page"
    return unleashed_api_get_request(url_base, page_number, url_param)

def get_invoices_helper(unleashed_data_name, start_date, end_date, url_param="", page_number=1):
    url_param = f"startDate={start_date}&endDate={end_date}"

    url_base = unleashed_data_name + "/Page"
    unleashed_json = unleashed_api_get_request(url_base, page_number, url_param)

    return unleashed_json

def get_stock_on_hand_helper(unleashed_data_name, end_date, url_param="", page_number=1):
    url_param = f"asAtDate={end_date}"

    url_base = unleashed_data_name + "/Page"
    unleashed_json = unleashed_api_get_request(url_base, page_number, url_param)

    return unleashed_json

def get_sales_orders(unleashed_data_name, start_date, end_date, url_param="", page_number=1):
    url_param = f"startDate={start_date}&endDate={end_date}"

    url_base = unleashed_data_name + "/Page"
    unleashed_json = unleashed_api_get_request(url_base, page_number, url_param)

    return unleashed_json







def clean_products_data(df):
    customer_df = df['ProductGroup'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['ProductGroup']), customer_df], axis=1)

    #rename GroupName to ProductGroup
    df_expanded.rename(columns={'GroupName': 'ProductGroup'}, inplace=True)

    return df_expanded

def clean_customers_data(df):
    return df

def clean_invoices_data(df):
    # The customer colum holds a dictionary, unpack the customer column so each part of dictionary is column
    # Expand the 'Customer' column into multiple columns
    customer_df = df['Customer'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['Customer']), customer_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Customer_LastModifiedOn'})

    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df_expanded.drop(columns=['InvoiceLines'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df_expanded[['InvoiceLines']].explode('InvoiceLines').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['InvoiceLines'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)
    final_df = final_df.rename(columns={'LastModifiedOn': 'InvoiceLines_LastModifiedOn'})

    print(f"Total invoices fetched after cleaning: {len(final_df)}")

    # convert weird date format to regular date
    final_df['InvoiceDate'] = final_df['InvoiceDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['DueDate'] = final_df['DueDate'].apply(convert_ms_date).dt.date.astype(str)
    # CAN PROBABLY REMOVE THIS ONE
    final_df['Customer_LastModifiedOn'] = final_df['Customer_LastModifiedOn'].apply(convert_ms_date).dt.date.astype(str)

    final_df['InvoiceLines_LastModifiedOn'] = final_df['InvoiceLines_LastModifiedOn'].apply(convert_ms_date).dt.date.astype(str)
    # final_df['LastModifiedOn'] = final_df['LastModifiedOn'].apply(convert_ms_date).dt.date.astype(str)

    return final_df


def clean_stock_on_hand(df):
    df['LastModifiedOn'] = df['LastModifiedOn'].apply(convert_ms_date).dt.date.astype(str)
    df['Bike_type'] = df['ProductDescription'].str.extract(r'^\s*(.*?)\s*-\s*')
    return df

def clean_sales_orders(df):

    customer_df = df['Customer'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['Customer']), customer_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Customer_LastModifiedOn'})


    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df_expanded.drop(columns=['SalesOrderLines'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df_expanded[['SalesOrderLines']].explode('SalesOrderLines').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['SalesOrderLines'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)
    final_df = final_df.rename(columns={'LastModifiedOn': 'SalesOrderLines_LastModifiedOn'})

    print(f"Total SalesOrders fetched after cleaning: {len(final_df)}")

    # convert weird date format to regular date
    final_df['OrderDate'] = final_df['OrderDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['RequiredDate'] = final_df['RequiredDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['CompletedDate'] = final_df['CompletedDate'].apply(convert_ms_date).dt.date.astype(str)
    return final_df


from concurrent.futures import ThreadPoolExecutor, as_completed


def get_page_data(unleashed_data_name, url_param, page_number, start_date='', end_date=''):
    if unleashed_data_name == 'Products':
        return get_products_helper(unleashed_data_name, url_param, page_number)
    elif unleashed_data_name == 'Invoices':
        return get_invoices_helper(unleashed_data_name, start_date, end_date, url_param, page_number)
    elif unleashed_data_name == 'StockOnHand':
        return get_stock_on_hand_helper(unleashed_data_name, end_date, url_param, page_number)
    elif unleashed_data_name == 'Customers':
        return get_customers_helper(unleashed_data_name, url_param, page_number)
    else:
        return get_sales_orders(unleashed_data_name, start_date, end_date, url_param, page_number)

def get_data_parallel(unleashed_data_name, url_param="", start_date='', end_date=''):
    # Get first page to determine total pages
    first_page = get_page_data(unleashed_data_name, url_param, 1, start_date, end_date)
    total_pages = first_page.get("Pagination", {}).get("NumberOfPages", 1)
    all_items = first_page.get("Items", [])

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(get_page_data, unleashed_data_name, url_param, page, start_date, end_date): page
            for page in range(2, total_pages + 1)
        }
        for future in as_completed(futures):
            page_data = future.result()
            items = page_data.get("Items", [])
            all_items.extend(items)

    df = pd.DataFrame(all_items)

    # Apply appropriate cleaning
    if unleashed_data_name == 'Products':
        df = clean_products_data(df)
    elif unleashed_data_name == 'Invoices':
        df = clean_invoices_data(df)
    elif unleashed_data_name == 'StockOnHand':
        df = clean_stock_on_hand(df)
    elif unleashed_data_name == 'Customers':
        df = clean_customers_data(df)
    else:
        df = clean_sales_orders(df)

    # file_path = fr"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_{unleashed_data_name}_data.xlsx"
    # os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # df.to_excel(file_path, index=False)
    # print(f"Excel file written to: {file_path}")

    return df

# start_date = '2025-01-04'
# end_date = '2025-08-04'
# df_invoices = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
