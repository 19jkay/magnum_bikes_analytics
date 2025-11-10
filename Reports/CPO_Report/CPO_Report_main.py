from Reports.CPO_Report.CPO_Report_Shopify_Stores import *
from Reports.CPO_Report.CPO_Report_Shopify_Online import *
from Reports.CPO_Report.CPO_Report_Unleashed import *
from Reports.CPO_Report.CPO_Report_Stock_Adjustment_CPOs import *
from Reports.CPO_Report.CPO_Report_Return_Reasons import *
import pandas as pd
import os
from datetime import datetime


start_date = "2025-01-01T00:00:00-00:00"
start_date_unleashed = '2024-01-01'

# Get today's date
today = datetime.now().date()   # use UTC date to match the -00:00 offset
end_date = today.strftime("%Y-%m-%dT23:59:59-00:00")

today = datetime.today()
end_date_unleashed = today.strftime('%Y-%m-%d')

print('Today\'s Date: ', end_date, "and ", end_date_unleashed)


df_shopify_stores_cpos = get_shopify_stores_CPOs(start_date, end_date)
df_shopify_online_cpos = get_shopify_online_CPOs(start_date, end_date)

df_full_shopify_cpos = pd.concat(
    [df_shopify_stores_cpos, df_shopify_online_cpos],
    axis=0,
    ignore_index=True  # resets the index so it’s clean
)

df_full_shopify_cpos['Final Price'] = df_full_shopify_cpos['Final Price'].round(2)
df_full_shopify_cpos.drop(columns=['Product line_name'], inplace=True)

df_unleashed_cpos, df_costco_sell_in = get_unleashed_costco_CPOs_and_sell_in(start_date_unleashed, end_date_unleashed)

df_costco_sell_in = df_costco_sell_in.loc[df_costco_sell_in['CustomerType'] == 'Costco'].copy()
df_costco_sell_in = df_costco_sell_in.loc[df_costco_sell_in['OrderQuantity'] >= 0]
df_costco_sell_in.drop(columns=['Status', 'CustomerCode', 'LineTotal', 'City', 'Region', 'Country', 'PostalCode', 'Cost'], inplace=True)

# Sample corrective row (adjust values as needed)
new_row = {
    "Number": "SI-CORRECTION",  # give it a unique identifier
    "CustomerName": "Costco",
    "Date": pd.Timestamp("2025-09-03"),  # same date as issue
    "ProductCode": "23150052",
    "ProductDescription": "Cosmo 2.0 T - Black- 48v 15 Ah",
    "OrderQuantity": -1588,   # correction
    "ProductGroup": "Bikes",
    "CustomerType": "Costco",
    "Type": "Invoice",
    "Date Year": 2025,
    "Date Month": "September",
    "Date MonthNum": 9,
    "Date Quarter": "Q3",
    "Bike Type": "Cosmo 2.0 T CPO"
}

# Convert to DataFrame
new_row_df = pd.DataFrame([new_row])
# Concatenate to your existing df_costco_sell_in
df_costco_sell_in = pd.concat([df_costco_sell_in, new_row_df], ignore_index=True)
print(df_costco_sell_in.tail())
print("Columns I Need: ", df_costco_sell_in.columns)
print("Sell-in: ", df_costco_sell_in['OrderQuantity'].sum())




df_unleashed_cpos.drop(columns=['Status', 'CustomerCode'], inplace=True)
df_unleashed_cpos_after = df_unleashed_cpos[df_unleashed_cpos['Date'] > '12/31/2024 0:00:00'].copy()

#costco returns lowriders cruisers I think
df_unleashed_costco = df_unleashed_cpos.loc[df_unleashed_cpos['CustomerType'] == 'Costco'].copy()
df_unleashed_costco = df_unleashed_costco.loc[df_unleashed_costco['OrderQuantity'] < 0]
print("Costco Returns: ", df_unleashed_costco['OrderQuantity'].sum())
#get stock adjustment returns
df_stock_adjustment_returns = Unleashed_PowerBI_Costco_Returns(start_date_unleashed, end_date_unleashed)
print('stock adjustment columns: ', df_stock_adjustment_returns.columns)


#CPO selling
df_unleashed_cpos_after = df_unleashed_cpos_after.loc[df_unleashed_cpos_after['CustomerType'] == 'Wholesale']

print("Costco1 quantity sum: ", df_unleashed_costco['OrderQuantity'].sum())








# Mapping from Unleashed → Shopify
unleashed_to_shopify = {
    'Number': 'order_id',
    'CustomerName' : 'Location',
    'Date': 'created_at',
    'ProductCode': 'Product Code',
    'ProductDescription': 'Product Description',
    'OrderQuantity': 'quantity',
    'LineTotal': 'Final Price',
    'ProductGroup': 'Product Group',
    'CustomerType': 'Customer Type',
    'City': 'City',
    'Region': 'State',
    'Country': 'Country',
    'PostalCode': 'Zip',
    'Date Year': 'created_at Year',
    'Date Month': 'created_at Month',
    'Date MonthNum': 'created_at MonthNum',
    'Date Quarter': 'created_at Quarter',
    # Extra Unleashed-only fields you may want to drop or keep separately:
    # 'Cost': ?, 'Type': ?
}

# Apply rename to unleashed DataFrame
df_unleashed_renamed = df_unleashed_cpos_after.rename(columns=unleashed_to_shopify)
# Align columns: keep only those that exist in Shopify schema
df_unleashed_aligned = df_unleashed_renamed[df_full_shopify_cpos.columns.intersection(df_unleashed_renamed.columns)]
# Now concatenate
df_full_shopify_cpos['Data Source'] = 'Shopify'
df_unleashed_aligned['Data Source'] = 'Unleashed'
df_combined = pd.concat([df_full_shopify_cpos, df_unleashed_aligned], ignore_index=True)
df_combined["order_id"] = df_combined["order_id"].astype(str)
df_combined["order_id"] = df_combined["order_id"].apply(lambda x: "'" + str(x))


#Costco Returns
df_unleashed_costco_renamed = df_unleashed_costco.rename(columns=unleashed_to_shopify)
df_unleashed_costco = df_unleashed_costco_renamed[df_full_shopify_cpos.columns.intersection(df_unleashed_costco_renamed.columns)]

keep_cols_costco_returns = ["order_id", "Location", "created_at", "quantity", "Bike Type", "Product Code", "Product Description", "Customer Type"]
df_unleashed_costco = df_unleashed_costco[keep_cols_costco_returns]

stock_adjustment_lept_cols = ['AdjustmentNumber', 'Location', 'Date', 'Return Quantity', 'Bike Type', 'ProductCode', 'ProductDescription', 'Customer Type']
df_stock_adjustment_returns = df_stock_adjustment_returns[stock_adjustment_lept_cols]

# Map stock adjustment column names to Costco schema
rename_map = {
    "AdjustmentNumber": "order_id",
    "Date": "created_at",
    "Return Quantity": "quantity",
    "ProductCode": "Product Code",
    "ProductDescription": "Product Description"
}
df_stock_adjustment_returns = df_stock_adjustment_returns.rename(columns=rename_map)
# Reorder to match Costco schema
df_stock_adjustment_returns = df_stock_adjustment_returns[keep_cols_costco_returns]
# Now you can concatenate
df_all_returns_costco = pd.concat([df_unleashed_costco, df_stock_adjustment_returns], ignore_index=True)

df_all_returns_costco["Date"] = pd.to_datetime(df_all_returns_costco["created_at"], errors="coerce", utc=True)
df_all_returns_costco["Date Year"] = df_all_returns_costco["Date"].dt.year
df_all_returns_costco["Date Month"] = df_all_returns_costco["Date"].dt.month_name()
df_all_returns_costco["Date MonthNum"] = df_all_returns_costco["Date"].dt.month
df_all_returns_costco["Date Quarter"] = df_all_returns_costco["Date"].dt.quarter
df_all_returns_costco["Date Quarter"] = "Q" + df_all_returns_costco["Date"].dt.quarter.astype(str)
df_all_returns_costco["Date"] = df_all_returns_costco["Date"].dt.tz_localize(None)

df_all_returns_costco.drop(columns=['Location', 'Customer Type', 'created_at'], inplace=True)
df_all_returns_costco['Positive Return Quantity'] = -df_all_returns_costco['quantity']

print("sum1: ", df_all_returns_costco['quantity'].sum())
print("sum2: ", df_all_returns_costco['Positive Return Quantity'].sum())


# file_path = fr"C:\Users\joshu\Documents\Shopify_API\shopify_orders_CPOs.xlsx"
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
# df_full_shopify_cpos.to_excel(file_path, index=False)
# print(f"Excel file written to: {file_path}")
#
# file_path = fr"C:\Users\joshu\Documents\Shopify_API\unleashed_orders_CPOs.xlsx"
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
# df_unleashed_cpos.to_excel(file_path, index=False)
# print(f"Excel file written to: {file_path}")

print(df_combined.dtypes)
df_combined["created_at"] = df_combined["created_at"].dt.strftime("%Y-%m-%d")

file_path = fr"C:\Users\joshu\Documents\Reporting\PowerBI_data\CPO_orders_shopify_unleashed.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_combined.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")

print(df_all_returns_costco.dtypes)
df_all_returns_costco["Date"] = df_all_returns_costco["Date"].dt.strftime("%Y-%m-%d")

file_path = fr"C:\Users\joshu\Documents\Reporting\PowerBI_data\costco_CPO_returns.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_all_returns_costco.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")

df_costco_sell_in["Date"] = df_costco_sell_in["Date"].dt.strftime("%Y-%m-%d")
file_path = fr"C:\Users\joshu\Documents\Reporting\PowerBI_data\costco_sell_in.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_costco_sell_in.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")

#SAVE COSTCO RETURN REASONS call function from CPO_Report_Return_Reasons
get_return_reason_data()
