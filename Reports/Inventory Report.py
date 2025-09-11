from datetime import datetime, timedelta

# from Unleashed_Data.Unleashed_Clean import Unleashed_invoices_clean_data
from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
import pandas as pd
import numpy as np
import os

# Today's date
today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

print("Today: ", today_str)
print("One year ago: ", one_year_ago_str)


reload_data = True
save_excel = False
#Get TTM (Trailing Twelve Months) data
df_SalesOrders = Unleashed_SalesOrders_clean_data_parallel(start_date=one_year_ago_str, end_date=today_str, reload=reload_data, save_excel=save_excel)

#Get Stock on Hand data
df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)


df_report = df_stockonhand[['ProductGroupName', 'ProductCode', 'ProductDescription', 'QtyOnHand', 'AvgCost']]

df_SalesOrders_grouped = df_SalesOrders.groupby(['ProductCode'])['OrderQuantity'].sum().reset_index()

#merge stockonhand and sales data
df_report = df_report.merge(df_SalesOrders_grouped, on='ProductCode', how='left')

#Convert 'QtyOnHand' to numeric column
df_report['QtyOnHand'] = pd.to_numeric(df_report['QtyOnHand'], errors='coerce')

# fill na with zeros so that blank sales entrees are zeros since there were no sales
df_report['OrderQuantity'] = pd.to_numeric(df_report['OrderQuantity'], errors='coerce')
df_report['OrderQuantity'] = df_report['OrderQuantity'].fillna(0)

#compute WOH
df_report['WOH'] = (df_report['QtyOnHand']/df_report['OrderQuantity']) * 52
# df_report['WOH'] = np.where(
#     df_report['OrderQuantity'].eq(0) | df_report['OrderQuantity'].isna(),
#     np.nan,
#     (df_report['QtyOnHand'] / df_report['OrderQuantity']) * 52
# )

df_report['Total Value'] = df_report['QtyOnHand'] * df_report['AvgCost']

#replace blanks with No Product Group in 'ProductGroupName'
df_report['ProductGroupName'] = df_report['ProductGroupName'].replace('', 'No Product Group')


#rename columns to non-technical names
df_report.rename(columns={'OrderQuantity': 'Units sold TTM'}, inplace=True)
df_report.rename(columns={'ProductGroupName': 'Product Group'}, inplace=True)
df_report.rename(columns={'ProductCode': 'Product Code'}, inplace=True)
df_report.rename(columns={'ProductDescription': 'Product Description'}, inplace=True)
df_report.rename(columns={'QtyOnHand': 'Qty On Hand'}, inplace=True)
df_report.rename(columns={'AvgCost': 'Avg Cost'}, inplace=True)

#get final
df_report = df_report[['Product Group', 'Product Code', 'Product Description', 'Qty On Hand', 'Units sold TTM', 'WOH', 'Avg Cost', 'Total Value']]


#get specific data
df_report_less_than = df_report.loc[df_report['WOH'] < 13].copy()
df_report_more_than = df_report.loc[(df_report['WOH'] > 104) | (df_report['Units sold TTM'].isna())].copy()

df_report = df_report.fillna("NaN")

# Define the path to your Excel file
# Set file path
file_path = fr"C:\Users\joshu\Documents\Reporting\Unleashed_Reports\Inventory_Report.xlsx"
with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_report.to_excel(writer, sheet_name="Sheet1", index=False)
print(f"Inventory Report file written to: {file_path}")






# numeric_cols = df_report.select_dtypes(include='number').columns
# df_report[['WOH']] = df_report[['WOH']].applymap(lambda x: 'NaN' if pd.isna(x) else x)
# df_report_more_than[['WOH']] = df_report_more_than[['WOH']].applymap(lambda x: 'NaN' if pd.isna(x) else x)

# df_report[['WOH']] = df_report[['WOH']].map(lambda x: 'NaN' if pd.isna(x) else x)
# df_report['WOH'] = df_report['WOH'].replace(r'^\s*$', np.nan, regex=True)

df_report_more_than[['WOH']] = df_report_more_than[['WOH']].map(lambda x: 'NaN' if pd.isna(x) else x)


#Greater than less than report
file_path = fr"C:\Users\joshu\Documents\Reporting\Unleashed_Reports\Inventory_Report_Low_High_WOH.xlsx"
with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_report_less_than.to_excel(writer, sheet_name="Less_Than_3_Months", index=False)
print(f"Inventory Report file written to: {file_path}")

with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_report_more_than.to_excel(writer, sheet_name="More_Than_2_Years", index=False)
print(f"Inventory Report file written to: {file_path}")





#Try to join purchase data with inventory
df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date='2025-01-10', end_date=today_str, reload=reload_data, save_excel=save_excel)

# df_PurchaseOrders = df_PurchaseOrders.loc[df_PurchaseOrders['OrderStatus'] != 'Complete'].copy()

df_PurchaseOrders = df_PurchaseOrders[['ProductCode', 'OrderNumber', 'OrderStatus', 'DeliveryDate', 'OrderQuantity']]
df_PurchaseOrders = df_PurchaseOrders.loc[df_PurchaseOrders['OrderStatus'] != 'Complete'].copy()
df_PurchaseOrders = df_PurchaseOrders.loc[df_PurchaseOrders['OrderQuantity'] != 0].copy()

#make every 'ProductCode' occur once and have the columns be the distinct orders

# Step 1: Add a sequential order number per ProductCode
df_PurchaseOrders['RepeatNum'] = df_PurchaseOrders.groupby('ProductCode').cumcount() + 1

# Step 2: Pivot OrderDate and OrderQuantity separately
order_numbers = df_PurchaseOrders.pivot(index='ProductCode', columns='RepeatNum', values='OrderNumber')
order_status = df_PurchaseOrders.pivot(index='ProductCode', columns='RepeatNum', values='OrderStatus')
delivery_dates = df_PurchaseOrders.pivot(index='ProductCode', columns='RepeatNum', values='DeliveryDate')
order_qtys  = df_PurchaseOrders.pivot(index='ProductCode', columns='RepeatNum', values='OrderQuantity')

# Step 3: Rename columns
order_numbers.columns = [f'OrderNumber_{i}' for i in order_numbers.columns]
order_status.columns = [f'OrderStatus_{i}' for i in order_status.columns]
delivery_dates.columns = [f'DeliveryDate_{i}' for i in delivery_dates.columns]
order_qtys.columns  = [f'OrderQuantity_{i}' for i in order_qtys.columns]

# Step 4: Interleave columns
interleaved_cols = []
for i in range(1, max(df_PurchaseOrders['RepeatNum']) + 1):
    interleaved_cols.extend([f'OrderNumber_{i}', f'OrderStatus_{i}', f'DeliveryDate_{i}', f'OrderQuantity_{i}'])

# Step 5: Combine and reorder
df_orders_wide = pd.concat([order_numbers, order_status, delivery_dates, order_qtys], axis=1)
df_orders_wide = df_orders_wide[interleaved_cols].reset_index()

#rename
df_orders_wide.rename(columns={'ProductCode': 'Product Code'}, inplace=True)

#Merge
df_inventory_purchases = df_report.merge(df_orders_wide, how='left', on='Product Code')


file_path = fr"C:\Users\joshu\Documents\Reporting\Unleashed_Reports\Inventory_Report_with_PurchaseOrders.xlsx"
folder_path = os.path.dirname(file_path)
os.makedirs(folder_path, exist_ok=True)
df_inventory_purchases.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")






