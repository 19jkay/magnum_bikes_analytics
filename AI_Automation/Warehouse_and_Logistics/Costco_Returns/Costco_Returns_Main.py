import pandas as pd
from datetime import datetime, timedelta
import os

#input: seriel number
#output: most recent Sales Order (SO) number, product description (whether black or teal)

from AI_Automation.Warehouse_and_Logistics.Costco_Returns.Costco_Returns_Helper import AI_Automation_SalesOrders_clean

today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# # Date exactly one year ago
# one_year_ago = today - timedelta(days=365)
# one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

start_date = '2023-01-01'
end_date = today_str


reload_data = False
save_excel = True

df_salesorders = AI_Automation_SalesOrders_clean(start_date, end_date, reload=reload_data, save_excel=save_excel)


print(df_salesorders.loc[df_salesorders['SerialNumber_Identifier'] == 'ZY25022844'])

df_product = df_salesorders.loc[df_salesorders['SerialNumber_Identifier'] == 'ZY25022844']

sales_order_number = df_product['SalesOrderNumber'].iloc[0]
product_description = df_product['ProductDescription'].iloc[0]
print("////////////")
print(sales_order_number)
print(product_description)

# print(df_salesorders.loc[df_salesorders['SerialNumber_Identifier'] == ''])







import requests
import hmac
import hashlib
import base64
import json
from datetime import datetime

# Replace with your actual credentials
API_KEY = os.getenv("unleashed_Magnum_Bikes_Analytics_api_key")
API_ID = os.getenv("unleashed_Magnum_Bikes_Analytics_api_id")
BASE_URL = 'https://api.unleashedsoftware.com/StockAdjustments'


def generate_signature(query_string):
    signature = hmac.new(API_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def create_stock_adjustment(product_code, warehouse_code, quantity, comment="Manual Adjustment"):
    # Build the payload
    payload = {
        "Warehouse": {
            "WarehouseCode": warehouse_code
        },
        "AdjustmentDate": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), #this line might not be correct
        "StockAdjustmentLines": [
            {
                "Product": {
                    "ProductCode": product_code
                },
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


warehouse_code = 'Costco Returns'
warehouse_guid = 'c59b7e06-2d4f-4bba-9208-48ed75594116'

comment = "Test with SO number: " + sales_order_number