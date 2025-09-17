import pandas as pd
from datetime import datetime, timedelta

from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from AI_Automation.Dashboards.Operations_Demand_Scorecard.Operations_Demand_Scorecard_helper import unwrap_sales_orders, unwrap_warehouse_sales_orders



today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
# print("one year ago: ", one_year_ago_str)

start_date = '2025-06-01'
# end_date = today_str
end_date = '2025-07-01' #for this and the reporting do to the first date of next month, so for january do 01-01 to 02-01
# end_date = today_str

reload_data = True
save_excel = False

# df_SalesOrders = Unleashed_SalesOrders_clean_data_parallel(start_date=one_year_ago_str, end_date=today_str, reload=reload_data, save_excel=save_excel)
df_SalesOrders = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
df_SalesOrders_dates = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date, end_date=end_date)
# df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)
# df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload_data, save_excel=save_excel)

# df_SalesOrders = df_SalesOrders[['CompletedDate', 'OrderStatus']]

#this says how many of each type of order was done, used for most of 'Unleashed Orders' block in Fulfilment section
status_counts = df_SalesOrders_dates['OrderStatus'].value_counts()
print("New Orders: ", len(df_SalesOrders_dates)) #GOT IT
print(status_counts)

print("///////////////////////")


#Now do 'Bikes' section in Fulfilment section
df_unwrapped_salesorders = unwrap_sales_orders(df_SalesOrders_dates) #unwrap all sales by SKU
df_unwrapped_salesorders_completed_date = unwrap_warehouse_sales_orders(df_SalesOrders)
# print("Sanity to check, num unique sales order: ", df_unwrapped_salesorders['OrderNumber'].nunique())
#this code matches a 'view sales orders' and 'sales enquiry' in Unleashed
df_unwrapped_salesorders_JamN_bikes = df_unwrapped_salesorders.loc[
    (df_unwrapped_salesorders['WarehouseName'] == 'Jam-N Logistics') &
    (df_unwrapped_salesorders['ProductGroup'] == 'Bikes')]

df_unwrapped_salesorders_completed_date_JamN_bikes = df_unwrapped_salesorders_completed_date.loc[
    (df_unwrapped_salesorders_completed_date['WarehouseName'] == 'Jam-N Logistics') &
    (df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Bikes')]

#not sure if I need the orders to be completed
df_unwrapped_salesorders_JamN_bikes = df_unwrapped_salesorders_JamN_bikes.loc[df_unwrapped_salesorders_JamN_bikes['OrderStatus'] == 'Completed']
df_unwrapped_salesorders_completed_date_JamN_bikes = df_unwrapped_salesorders_completed_date_JamN_bikes.loc[df_unwrapped_salesorders_completed_date_JamN_bikes['OrderStatus'] == 'Completed']
print("Total # of Bike Orders: Jam-N: ", df_unwrapped_salesorders_JamN_bikes['OrderNumber'].nunique())
print("Total # of Bike Orders: Jam-N 2: ", df_unwrapped_salesorders_completed_date_JamN_bikes['OrderNumber'].nunique())
print("Total number of bikes from JamN: ", df_unwrapped_salesorders_JamN_bikes['OrderQuantity'].sum())
print("Total number of bikes from JamN 2: ", df_unwrapped_salesorders_completed_date_JamN_bikes['OrderQuantity'].sum())


print("/////////////////////")

#Now do accessories section
df_unwrapped_salesorders_accessories = df_unwrapped_salesorders.loc[df_unwrapped_salesorders['ProductGroup'] == 'Accessories']
df_unwrapped_salesorders_accessories_completed_date = df_unwrapped_salesorders_completed_date.loc[df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Accessories']
print("Total Accessories Orders: ", df_unwrapped_salesorders_accessories['OrderNumber'].nunique())
print("Total Accessories Orders 2: ", df_unwrapped_salesorders_accessories_completed_date['OrderNumber'].nunique())
print("Accessory Units Fulfilled: ", df_unwrapped_salesorders_accessories['OrderQuantity'].sum())
print("Accessory Units Fulfilled 2: ", df_unwrapped_salesorders_accessories_completed_date['OrderQuantity'].sum())



