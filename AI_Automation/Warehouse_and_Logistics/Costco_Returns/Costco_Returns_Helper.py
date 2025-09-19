# from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from AI_Automation.Warehouse_and_Logistics.Costco_Returns.Costco_Returns_Get import get_data_parallel
import os
import pandas as pd
import requests
import hmac
import hashlib
import base64
import json
from datetime import datetime, timezone

from datetime import datetime


def AI_Automation_SalesOrders_clean(start_date, end_date, reload=True, save_excel=False):
    # This code basically gets a Sales Enquiry from Unleashed with Transaction Date = Completed Date and everything else default

    # rerun API Calls
    if reload:
        df_salesorders = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)

        df_salesorders.rename(columns={'Product.ProductCode': 'ProductCode',
                                       'Product.ProductDescription': 'ProductDescription'}, inplace=True)

        df_salesorders['LineTotal'] = df_salesorders['LineTotal'].astype(float)  # convert 'Sub Total' column to float datatype

        # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
        # Step 1: Create a copy without the 'InvoiceLines' column
        base_df = df_salesorders.drop(columns=['SerialNumbers'])
        # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
        lines_df = df_salesorders[['SerialNumbers']].explode('SerialNumbers').reset_index()
        # Step 3: Normalize each dictionary into its own row
        salesorder_lines_expanded = pd.json_normalize(lines_df['SerialNumbers'])
        # Step 4: Merge base invoice info with each invoice line
        final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), salesorder_lines_expanded], axis=1)
        # final_df = final_df.rename(columns={'LastModifiedOn': 'SalesOrderLines_LastModifiedOn'})
        final_df = final_df.rename(columns={'LastModifiedOn': 'SarielNumber_LastModifiedOn'})
        final_df = final_df.rename(columns={'Guid': 'SerialNumber_Guid'})
        final_df = final_df.rename(columns={'Identifier': 'SerialNumber_Identifier'})


        final_df = final_df[['OrderNumber', 'CustomerCode', 'CustomerName', 'OrderDate', 'RequiredDate', 'CompletedDate', 'ProductCode', 'ProductDescription', 'SerialNumber_Identifier', 'SerialNumber_Guid', 'SarielNumber_LastModifiedOn']]
        final_df = final_df.rename(columns={'OrderNumber': 'SalesOrderNumber'})

        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_serialnumbers_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            final_df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        SALESORDERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_serialnumbers_data.xlsx"
        final_df = pd.read_excel(SALESORDERS_FILENAME)

    return final_df



API_KEY = os.getenv("unleashed_Magnum_Bikes_Analytics_api_key")
API_ID = os.getenv("unleashed_Magnum_Bikes_Analytics_api_id")
BASE_URL = 'https://api.unleashedsoftware.com/StockAdjustments'


def generate_signature(query_string):
    signature = hmac.new(API_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def create_stock_adjustment(product_code, serial_number, warehouse_code, quantity, comment="Manual Adjustment"):

    # Build the payload
    payload = {
        "Warehouse": {
            "WarehouseCode": warehouse_code
        },
        "AdjustmentDate": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "AdjustmentReason": "Adjustment",
        "Status": "Completed", #if does not work delete this
        "StockAdjustmentLines": [
            {
                "Product": {
                    "ProductCode": product_code
                },
                "SerialNumbers": [ #and if code does not work delete this
                    {
                        "Identifier": serial_number
                    }
                ],
                "NewQuantity": quantity,
                "NewActualValue": 0,
                "Comments": comment
            }
        ]
    }

    # Prepare headers
    query_string = ""  # No query parameters for POST
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-auth-id": API_ID,
        "api-auth-signature": generate_signature(query_string)
    }

    # Send POST request
    response = requests.post(BASE_URL, headers=headers, json=payload)

    if response.status_code == 201:
        print("✅ Stock adjustment created successfully.")
        return response.json()
    else:
        print(f"❌ Failed to create stock adjustment: {response.status_code}")
        print(response.text)
        return None