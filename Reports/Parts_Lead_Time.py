from datetime import datetime, timedelta

# from Unleashed_Data.Unleashed_Clean import Unleashed_invoices_clean_data
from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
import pandas as pd

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
print("SalesOrders Loaded")

#Get Stock on Hand data
df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)
print("StockOnHand Loaded")

df_report = df_stockonhand[['ProductGroupName', 'ProductCode', 'ProductDescription', 'QtyOnHand', 'AvgCost']]

df_SalesOrders_grouped = df_SalesOrders.groupby(['ProductCode'])['OrderQuantity'].sum().reset_index()

df_report = df_report.merge(df_SalesOrders_grouped, on='ProductCode', how='left')


df_report['WOH'] = (df_report['QtyOnHand']/df_report['OrderQuantity']) * 52
df_report['Total Value'] = df_report['QtyOnHand'] * df_report['AvgCost']
df_report['ProductGroupName'] = df_report['ProductGroupName'].replace('', 'No Product Group')


df_report.rename(columns={'OrderQuantity': 'Units sold TTM'}, inplace=True)
df_report.rename(columns={'ProductGroupName': 'Product Group'}, inplace=True)
df_report.rename(columns={'ProductCode': 'Product Code'}, inplace=True)
df_report.rename(columns={'ProductDescription': 'Product Description'}, inplace=True)
df_report.rename(columns={'QtyOnHand': 'Qty On Hand'}, inplace=True)
df_report.rename(columns={'AvgCost': 'Avg Cost'}, inplace=True)

df_report = df_report[['Product Group', 'Product Code', 'Product Description', 'Qty On Hand', 'Units sold TTM', 'WOH', 'Avg Cost', 'Total Value']]

parts_groups = ['Battery',
                'Bottom Brackets', 'Brakes', 'Chargers',
                'Cockpit', 'Controllers', 'Conversion Kit', 'Derailleur Hangers',
                'Displays', 'Drivetrain', 'Electronics', 'Fenders', 'Forks', 'Frame',
                'Headset', 'Lights', 'Motor Wheels', 'Motors',
                'Racks', 'Scooters', 'Shifters', 'Throttles', 'Tires', 'Tubes',
                'Wheels', 'Derailleurs']


df_report_parts = df_report.loc[df_report['Product Group'].isin(parts_groups)].copy()
df_report_parts = df_report_parts.loc[df_report_parts['Units sold TTM'] > 0].copy()

df_report_parts['Lead Time Weeks'] = 13 #13 for 13 weeks taking place in 3 months
df_report_parts['Reorder_Flag'] = df_report_parts['WOH'] <= df_report_parts['Lead Time Weeks']


# Define the path to your Excel file
# Set file path
file_path = fr"C:\Users\joshu\Documents\Reporting\Unleashed_Reports\Parts_Lead_Time.xlsx"
with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_report_parts.to_excel(writer, sheet_name="Sheet1", index=False)

print(f"Parts Lead Time Report file written to: {file_path}")