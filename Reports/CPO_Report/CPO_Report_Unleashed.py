import hmac
import json
import hashlib
import base64
import requests
import os
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta


#need invoices, credits, stock_adjustments

def configure():
    load_dotenv()


unleashed_api_key = os.getenv("unleashed_Magnum_Bikes_Analytics_api_key")
unleashed_api_id = os.getenv("unleashed_Magnum_Bikes_Analytics_api_id")


def convert_ms_date(ms_date_str):
    import re
    from datetime import datetime
    match = re.search(r'\d+', str(ms_date_str))
    if match:
        timestamp_ms = int(match.group())
        return datetime.utcfromtimestamp(timestamp_ms / 1000)
    return None  # Handle cases like NaN or malformed strings


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
    url_param = f"startDate={start_date}&endDate={end_date}&invoiceStatus=Completed"

    url_base = unleashed_data_name + "/Page"
    unleashed_json = unleashed_api_get_request(url_base, page_number, url_param)

    return unleashed_json


def get_stock_adjustment_data(unleashed_data_name, adjustment_date, url_param="", page_number=1):
    url_param = f"adjustmentDate={adjustment_date}"

    url_base = unleashed_data_name + "/Page"
    unleashed_json = unleashed_api_get_request(url_base, page_number, url_param)

    return unleashed_json

def get_credit_notes_data(unleashed_data_name, start_date, end_date, url_param="", page_number=1):
    url_param = f"startDate={start_date}&endDate={end_date}&creditStatus=Completed"

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
    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df.drop(columns=['Addresses'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df[['Addresses']].explode('Addresses').reset_index()
    # Step 3: Normalize each dictionary into its own row
    customers_lines_expanded = pd.json_normalize(lines_df['Addresses'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), customers_lines_expanded], axis=1)
    final_df = final_df.rename(columns={'LastModifiedOn': 'Addresses_LastModifiedOn'})

    final_df['CreatedOn'] = final_df['CreatedOn'].apply(convert_ms_date).dt.date.astype(str)

    return final_df


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


def clean_stock_adjustment_data(df):
    warehouse_df = df['Warehouse'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['Warehouse']), warehouse_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Warehouse_LastModifiedOn'})

    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df_expanded.drop(columns=['StockAdjustmentLines'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df_expanded[['StockAdjustmentLines']].explode('StockAdjustmentLines').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['StockAdjustmentLines'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)

    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = final_df.drop(columns=['SerialNumbers'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = final_df[['SerialNumbers']].explode('SerialNumbers').reset_index()
    # Step 3: Normalize each dictionary into its own row
    serial_lines_expanded = pd.json_normalize(lines_df['SerialNumbers'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), serial_lines_expanded], axis=1)
    final_df.rename(columns={'Identifier': 'SerialNumber'}, inplace=True)



    final_df['AdjustmentDate'] = final_df['AdjustmentDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['CreatedOn'] = final_df['CreatedOn'].apply(convert_ms_date).dt.date.astype(str)
    # final_df['LastModifiedOn'] = final_df['LastModifiedOn'].apply(convert_ms_date).dt.date.astype(str)
    return final_df

def clean_credit_notes_data(df):
    customer_df = df['Customer'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['Customer']), customer_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Customer_LastModifiedOn'})
    df_expanded = df_expanded.rename(columns={'Guid': 'Customer_Guid'})

    warehouse_df = df_expanded['Warehouse'].apply(pd.Series)
    df_expanded = pd.concat([df_expanded.drop(columns=['Warehouse']), warehouse_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Warehouse_LastModifiedOn'})
    df_expanded = df_expanded.rename(columns={'Guid': 'Warehouse_Guid'})

    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df_expanded.drop(columns=['CreditLines'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df_expanded[['CreditLines']].explode('CreditLines').reset_index()
    # Step 3: Normalize each dictionary into its own row
    credit_lines_expanded = pd.json_normalize(lines_df['CreditLines'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), credit_lines_expanded], axis=1)

    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = final_df.drop(columns=['SerialNumbers'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = final_df[['SerialNumbers']].explode('SerialNumbers').reset_index()
    # Step 3: Normalize each dictionary into its own row
    serial_lines_expanded = pd.json_normalize(lines_df['SerialNumbers'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), serial_lines_expanded], axis=1)
    final_df.rename(columns={'Identifier': 'SerialNumber'}, inplace=True)

    final_df['CreatedOn'] = final_df['CreatedOn'].apply(convert_ms_date).dt.date.astype(str)
    final_df['CreditDate'] = final_df['CreditDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['SalesInvoiceDate'] = final_df['SalesInvoiceDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['RequiredDeliveryDate'] = final_df['RequiredDeliveryDate'].apply(convert_ms_date).dt.date.astype(str)

    return final_df


def get_page_data(unleashed_data_name, url_param, page_number, start_date='', end_date=''):
    if unleashed_data_name == 'Invoices':
        return get_invoices_helper(unleashed_data_name, start_date, end_date, url_param, page_number)
    elif unleashed_data_name == 'Products':
        return get_products_helper(unleashed_data_name, url_param, page_number)
    elif unleashed_data_name == 'StockAdjustments':
        return get_stock_adjustment_data(unleashed_data_name, start_date, url_param, page_number)
    elif unleashed_data_name == 'CreditNotes':
        return get_credit_notes_data(unleashed_data_name, start_date, end_date, url_param, page_number)
    elif unleashed_data_name == 'Customers':
        return get_customers_helper(unleashed_data_name, url_param, page_number)
    else:
        return

def get_data_parallel(unleashed_data_name, url_param="", start_date='', end_date=''):
    # Get first page to determine total pages

    # if unleashed_data_name == 'SalesOrdersDate': unleashed_data_name = 'SalesOrders'

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
    if unleashed_data_name == 'Invoices':
        # file_path = fr"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_new_main_test_thing_data.xlsx"
        # os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # df.to_excel(file_path, index=False)
        # print(f"Excel file written to: {file_path}")
        # print("DID IT")
        # print("Sub-total sum: ", df['SubTotal'].sum())
        df = clean_invoices_data(df)
    elif unleashed_data_name == 'Products':
        df = clean_products_data(df)
    elif unleashed_data_name == 'StockAdjustments':
        df = clean_stock_adjustment_data(df)
    elif unleashed_data_name == 'Customers':
        df = clean_customers_data(df)
    elif unleashed_data_name == 'CreditNotes':
        df = clean_credit_notes_data(df)
    else:
        df = ''

    return df






def Unleashed_Invoices_clean_data_parallel(start_date, end_date, df_products, df_customers):

    df_invoices = get_data_parallel(unleashed_data_name="Invoices", start_date=start_date, end_date=end_date)



    # df_products = df_products[['ProductCode', 'ProductGroup', 'AverageLandPrice']]
    #
    # # df_customers = df_customers[['CustomerCode', 'CustomerType']]
    # df_customers = df_customers.loc[df_customers['AddressType'] == 'Postal']
    # df_customers = df_customers.drop_duplicates(subset='CustomerCode', keep='first')
    # df_customers = df_customers[['CustomerCode', 'CustomerType', 'City', 'Region', 'Country', 'PostalCode']]

    # df_salesorders = df_salesorders.loc[df_salesorders['OrderStatus'] == 'Completed']  # get completed invoices

    df_invoices.rename(columns={'Product.ProductCode': 'ProductCode',
                                   'Product.ProductDescription': 'ProductDescription'}, inplace=True)

    df_invoices = df_invoices[['InvoiceStatus', 'InvoiceNumber', 'CustomerCode', 'CustomerName', 'InvoiceDate', 'ProductCode', 'ProductDescription', 'OrderQuantity', 'LineTotal']]

    df = df_invoices.merge(
        df_products[['ProductCode', 'ProductGroup', 'AverageLandPrice']],
        on='ProductCode',
        how='left'
    )

    df = df.merge(
        df_customers[['CustomerCode', 'CustomerType', 'City', 'Region', 'Country', 'PostalCode']],
        on='CustomerCode',
        how='left'
    )

    df['Cost'] = df['AverageLandPrice'] * df['OrderQuantity']
    df = df.drop('AverageLandPrice', axis=1)

    # print(f"Clean len before: {len(df)}")
    # print("Clean Sum of sales before: ", df['LineTotal'].sum())

    # df = df.loc[~df['InvoiceDate'].isna()]  # remove nan dates
    df['LineTotal'] = df['LineTotal'].astype(float)  # convert 'Sub Total' column to float datatype
    # df_invoices['OrderQuantity'] = df_invoices['OrderQuantity'].str.replace(',', '').astype(float)
    df['OrderQuantity'] = df['OrderQuantity'].astype(float)
    df['ProductGroup'] = df['ProductGroup'].replace('', 'No Product Group')
    df['ProductGroup'] = df['ProductGroup'].fillna('No Product Group')

    df['CustomerType'] = df['CustomerType'].replace('', 'No Customer Type')
    df['CustomerType'] = df['CustomerType'].fillna('No Customer Type')

    df['Type'] = 'Invoice'


    # # invoices
    # ['InvoiceStatus', 'InvoiceNumber', 'CustomerCode', 'CustomerName', 'InvoiceDate', 'ProductCode',
    #  'ProductDescription', 'OrderQuantity',
    #  'LineTotal']
    #
    # # credit notes
    # ['Status', 'CreditNoteNumber', 'CustomerCode', 'CustomerName', 'CreditDate', 'ProductCode', 'ProductDescription',
    #  'CreditQuantity', 'LineTotal']]

    df.rename(columns={'InvoiceStatus' : 'Status', 'InvoiceNumber' : 'Number', 'InvoiceDate' : 'Date'}, inplace=True)
    df = df.loc[df['Status'] == 'Completed']
    return df

def Unleashed_credit_note_clean_data_parallel(start_date, end_date, df_products, df_customers):
    df_credit_notes = get_data_parallel(unleashed_data_name='CreditNotes', start_date=start_date, end_date=end_date)


    df_credit_notes.rename(columns={'Product.ProductCode': 'ProductCode',
                                'Product.ProductDescription': 'ProductDescription'}, inplace=True)

    # df_credit_notes = df_credit_notes[
    #     ['Status', 'InvoiceNumber', 'CustomerCode', 'CustomerName', 'InvoiceDate', 'ProductCode', 'ProductDescription',
    #      'OrderQuantity',
    #      'LineTotal']]

    df_credit_notes = df_credit_notes[['Status', 'CreditNoteNumber', 'CustomerCode', 'CustomerName', 'CreditDate', 'ProductCode', 'ProductDescription', 'CreditQuantity', 'LineTotal']]

    df = df_credit_notes.merge(
        df_products[['ProductCode', 'ProductGroup', 'AverageLandPrice']],
        on='ProductCode',
        how='left'
    )

    df = df.merge(
        df_customers[['CustomerCode', 'CustomerType', 'City', 'Region', 'Country', 'PostalCode']],
        on='CustomerCode',
        how='left'
    )

    df['Cost'] = df['AverageLandPrice'] * df['CreditQuantity']
    df = df.drop('AverageLandPrice', axis=1)

    # df = df.loc[~df['InvoiceDate'].isna()]  # remove nan dates
    df['LineTotal'] = df['LineTotal'].astype(float)  # convert 'Sub Total' column to float datatype
    # df_invoices['OrderQuantity'] = df_invoices['OrderQuantity'].str.replace(',', '').astype(float)
    df['CreditQuantity'] = df['CreditQuantity'].astype(float)
    df['ProductGroup'] = df['ProductGroup'].replace('', 'No Product Group')
    df['ProductGroup'] = df['ProductGroup'].fillna('No Product Group')

    df['CustomerType'] = df['CustomerType'].replace('', 'No Customer Type')
    df['CustomerType'] = df['CustomerType'].fillna('No Customer Type')


    #align with invoice column names
    df['Type'] = 'Credit'
    # df['CredtiQuantity'] = -df['CreditQuantity']

    df.rename(columns={'CreditNoteNumber': 'Number',
                       'CreditDate' : 'Date',
                       'CreditQuantity' : 'OrderQuantity'}, inplace=True)

    #turn everything negative because they are credits
    df['OrderQuantity'] = -df['OrderQuantity']
    df['LineTotal'] = -df['LineTotal']

    # df = df.loc[df['Status'] == 'Completed']
    df = df.drop_duplicates(keep="first")

    return df

def get_unleashed_costco_CPOs_and_sell_in(start_date, end_date):

# start_date = '2025-01-01'
# end_date = '2025-10-31'

    #get and clean product, customer data
    df_products = get_data_parallel(unleashed_data_name="Products")
    df_customers = get_data_parallel(unleashed_data_name="Customers")
    df_products = df_products[['ProductCode', 'ProductGroup', 'AverageLandPrice']]
    df_customers = df_customers.loc[df_customers['AddressType'] == 'Postal']
    df_customers = df_customers.drop_duplicates(subset='CustomerCode', keep='first')
    df_customers = df_customers[['CustomerCode', 'CustomerType', 'City', 'Region', 'Country', 'PostalCode']]

    df_invoices = Unleashed_Invoices_clean_data_parallel(start_date, end_date, df_products, df_customers)
    df_credits = Unleashed_credit_note_clean_data_parallel(start_date, end_date, df_products, df_customers)

    df = pd.concat([df_invoices, df_credits], axis=0)

    # convert dates to proper format
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df["Date Year"] = df["Date"].dt.year
    df["Date Month"] = df["Date"].dt.month_name()
    df["Date MonthNum"] = df["Date"].dt.month
    df["Date Quarter"] = df["Date"].dt.quarter
    df["Date Quarter"] = "Q" + df["Date"].dt.quarter.astype(str)
    df["Date"] = df["Date"].dt.tz_localize(None)

    # print("Invoices sum Line Total: ", df_invoices['LineTotal'].sum())
    # print("Credits sum Line Total: ", df_credits['LineTotal'].sum())
    # print("Credit quantity sum: ", df_credits['OrderQuantity'].sum())
    #
    # #THIS WORKED AND GOT INVOICES WITHIN Big O($100) of unleashed
    # df_thing = df_invoices.loc[(~df_invoices['ProductCode'].isin(['Freight', 'Shipping'])) & (~df_invoices['ProductCode'].isna())]
    # print("Invoice Sum without shipping and handling: ", df_thing['LineTotal'].sum())
    #
    #
    df_match_unleashed = df.loc[(~df['ProductCode'].isin(['Freight', 'Shipping'])) & (~df['ProductCode'].isna())]
    # print("Everything Sum without shipping and handling: ", df_match_unleashed['LineTotal'].sum())
    #


    #GET CPO PRODUCT
    cpo_lowrider_graphite_product_code = 'Low Rider BLK-GPH'
    cpo_lowrider_graphite_product_description = 'CPO Low rider 2.0 - Black-Graphite - 48v 15Ah'
    cpo_lowrider_copper_product_code = 'Low Rider - BLK-CPR'
    cpo_lowrider_copper_product_description = 'Low rider 2.0 - Black-Copper - 48v 15Ah'

    cpo_cosmo_black_product_code = 'CPO23150052'
    cpo_cosmo_black_product_description = 'CPO - Cosmo 2.0 T - Black- 48v 15 Ah'
    cpo_cosmo_calypso_product_code = 'CPO23150051'
    cpo_cosmo_calypso_product_description = 'CPO - Cosmo 2.0 T - Calypso - 48v 15 Ah'

    #Copper, Graphite
    cpo_cruiser_graphite_product_code = 'Cruiser BLK-GPH'
    cpo_cruiser_graphite_product_description = 'Cruiser 2.0 - Black-Gunmetal - 48v 15Ah'
    cpo_cruiser_copper_product_code = 'Cruiser BLK-CPR'
    cpo_cruiser_copper_product_description = 'Cruiser 2.0 - Black-Copper - 48v 15Ah'

    cpo_product_codes = [
        cpo_lowrider_graphite_product_code,
        cpo_lowrider_copper_product_code,
        cpo_cosmo_black_product_code,
        cpo_cosmo_calypso_product_code,
        cpo_cruiser_graphite_product_code,
        cpo_cruiser_copper_product_code
    ]

    # List of product description variables
    cpo_product_descriptions = [
        cpo_lowrider_graphite_product_description,
        cpo_lowrider_copper_product_description,
        cpo_cosmo_black_product_description,
        cpo_cosmo_calypso_product_description,
        cpo_cruiser_graphite_product_description,
        cpo_cruiser_copper_product_description
    ]


    df_cpos = df_match_unleashed.loc[df_match_unleashed['ProductCode'].isin(cpo_product_codes)]

    low_rider_product_codes = ['Low Rider BLK-GPH', 'Low Rider - BLK-CPR']
    df_cpos.loc[df_cpos['ProductCode'].isin(low_rider_product_codes), 'Bike Type'] = 'Low Rider 2.0 CPO'

    cosmo_product_codes = ['CPO23150052', 'CPO23150051']
    df_cpos.loc[df_cpos['ProductCode'].isin(cosmo_product_codes), 'Bike Type'] = 'Cosmo 2.0 T CPO'

    cruiser_product_codes = ['Cruiser BLK-GPH', 'Cruiser BLK-CPR']
    df_cpos.loc[df_cpos['ProductCode'].isin(cruiser_product_codes), 'Bike Type'] = 'Cruiser 2.0 CPO'














    #GET REGULAR COSTCO SELL IN Product codes
    lowrider_graphite_product_code = 'Low Rider BLK-GPH'
    lowrider_copper_product_code = 'Low Rider - BLK-CPR'

    cosmo_black_product_code = '23150052'
    cosmo_calypso_product_code = '23150051'

    cruiser_graphite_product_code = 'Cruiser BLK-GPH'
    cruiser_copper_product_code = 'Cruiser BLK-CPR'

    costco_sell_in_product_codes = [
                lowrider_graphite_product_code,
                lowrider_copper_product_code,
                cosmo_black_product_code,
                cosmo_calypso_product_code,
                cruiser_graphite_product_code,
                cruiser_copper_product_code
            ]

    df_costco_sell_in = df_match_unleashed.loc[df_match_unleashed['ProductCode'].isin(costco_sell_in_product_codes)]

    #standard costco bikes sell-in
    low_rider_product_codes = [lowrider_graphite_product_code, lowrider_copper_product_code]
    df_costco_sell_in.loc[df_costco_sell_in['ProductCode'].isin(low_rider_product_codes), 'Bike Type'] = 'Low Rider 2.0 CPO'

    cosmo_product_codes = [cosmo_black_product_code, cosmo_calypso_product_code]
    df_costco_sell_in.loc[df_costco_sell_in['ProductCode'].isin(cosmo_product_codes), 'Bike Type'] = 'Cosmo 2.0 T CPO'

    cruiser_product_codes = [cruiser_graphite_product_code, cruiser_copper_product_code]
    df_costco_sell_in.loc[df_costco_sell_in['ProductCode'].isin(cruiser_product_codes), 'Bike Type'] = 'Cruiser 2.0 CPO'

    #I guess costco lowrider and cruiser sell in happens before 2025 and Cosmo 2.0 sell in happens during 2025
    #actually I think since its Costco sell in the product code takes care of this, at least for a sales enquiry, maybe credits undoes this
    #actually if it is pulled with invoices and credits, just take the positives, the negatives are returns.
    #last lowrider BLK-GPH sell-in has order date of 1/28/2025 with quantity 120
    #last lowrider BLK-CPR sell-in has order date 1/28/2025 with quantity 32
    #last cruiser BLK-CPR has order date 4/22/2025 with quantity 28
    #last cruiser BLK-GPH has order date 4/22/2025 with quantity 115




    return df_cpos, df_costco_sell_in

# df_orders_clean.loc[
#         mask_cosmo & df_orders_clean['line_name'].str.contains("black", case=False, na=False),
#         'Product Code'
#     ] = 'CPO23150052'
#     df_orders_clean.loc[
#         mask_cosmo & df_orders_clean['line_name'].str.contains("black", case=False, na=False),
#         'Product Description'
#     ] = 'CPO - Cosmo 2.0 T - Black- 48v 15 Ah'


# file_path = fr"C:\Users\joshu\Documents\Shopify_API\Unleashed_CPO_Invoices.xlsx"
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
# df_cpos.to_excel(file_path, index=False)
# print(f"Excel file written to: {file_path}")
# print("DID IT")