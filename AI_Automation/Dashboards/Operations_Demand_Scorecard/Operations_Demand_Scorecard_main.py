import pandas as pd
from datetime import datetime, timedelta

from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from AI_Automation.Dashboards.Operations_Demand_Scorecard.Operations_Demand_Scorecard_helper import unwrap_sales_orders, unwrap_warehouse_sales_orders, get_parts_list



today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
# print("one year ago: ", one_year_ago_str)

start_date = '2025-08-01'
end_date = '2025-09-01' #always do one more here than you do in unleashed view sales orders

reload_data = True
save_excel = True



df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload_data, save_excel=save_excel)



df_SalesOrders_completed_dates = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
df_SalesOrders_order_dates = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date, end_date=end_date)

df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

# df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload_data, save_excel=save_excel)

# df_SalesOrders = df_SalesOrders[['CompletedDate', 'OrderStatus']]

#this says how many of each type of order was done, used for most of 'Unleashed Orders' block in Fulfilment section
print("UNLEASHED ORDERS")
print("New Orders: ", len(df_SalesOrders_order_dates)) #GOT IT
print("Completed Orders: ", df_SalesOrders_completed_dates['OrderNumber'].nunique())

status_counts = df_SalesOrders_order_dates['OrderStatus'].value_counts()
print(status_counts)

print("///////////////////////")



#Bikes section of Fulfillment section
print("BIKES")
df_unwrapped_salesorders_order_dates = unwrap_sales_orders(df_SalesOrders_order_dates)
df_unwrapped_salesorders_JamN_bikes_order_dates = df_unwrapped_salesorders_order_dates.loc[
    (df_unwrapped_salesorders_order_dates['WarehouseName'] == 'Jam-N Logistics') &
    (df_unwrapped_salesorders_order_dates['ProductGroup'] == 'Bikes')]
print("Total # of Bike Orders: Jam-N: ", df_unwrapped_salesorders_JamN_bikes_order_dates['OrderNumber'].nunique())

df_unwrapped_salesorders_completed_date = unwrap_warehouse_sales_orders(df_SalesOrders_completed_dates)
df_unwrapped_salesorders_completed_date_JamN_bikes = df_unwrapped_salesorders_completed_date.loc[
    (df_unwrapped_salesorders_completed_date['WarehouseName'] == 'Jam-N Logistics') &
    (df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Bikes')]
df_unwrapped_salesorders_completed_date_JamN_bikes = df_unwrapped_salesorders_completed_date_JamN_bikes.loc[df_unwrapped_salesorders_completed_date_JamN_bikes['OrderStatus'] == 'Completed']
print("Total number of bikes from JamN 2: ", df_unwrapped_salesorders_completed_date_JamN_bikes['OrderQuantity'].sum())



print("/////////////////////")
print("ACCESSORIES")
#Now do accessories section
df_unwrapped_salesorders_accessories_order_dates = df_unwrapped_salesorders_order_dates.loc[df_unwrapped_salesorders_order_dates['ProductGroup'] == 'Accessories']
print("Total Accessories Orders: ", df_unwrapped_salesorders_accessories_order_dates['OrderNumber'].nunique())

df_unwrapped_salesorders_accessories_completed_date = df_unwrapped_salesorders_completed_date.loc[df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Accessories']
# print("Total Accessories Orders 2: ", df_unwrapped_salesorders_accessories_completed_date['OrderNumber'].nunique())
# print("Accessory Units Fulfilled: ", df_unwrapped_salesorders_accessories['OrderQuantity'].sum())
print("Accessory Units Fulfilled 2: ", df_unwrapped_salesorders_accessories_completed_date['OrderQuantity'].sum())




print("/////////////////////")
print("Inventory Management")
print("Inventory Health")

df_stockonhand_bikes = df_stockonhand.loc[df_stockonhand['ProductGroupName'] == 'Bikes']
print("Bikes in stock: ", df_stockonhand_bikes['QtyOnHand'].sum())

df_unwrapped_salesorders_order_dates_backorder_bikes = df_unwrapped_salesorders_order_dates.loc[
    (df_unwrapped_salesorders_order_dates['OrderStatus'] == 'Backordered')
    & (df_unwrapped_salesorders_order_dates['ProductGroup'] == 'Bikes')]
print("Bikes in back orders: ", df_unwrapped_salesorders_order_dates_backorder_bikes['OrderQuantity'].sum())

df_stockonhand_parts = df_stockonhand.loc[df_stockonhand['ProductGroupName'].isin(get_parts_list())]
print("Parts in stock: ", df_stockonhand_parts['QtyOnHand'].sum())

df_unwrapped_salesorders_order_dates_backorder_parts = df_unwrapped_salesorders_order_dates.loc[
    (df_unwrapped_salesorders_order_dates['OrderStatus'] == 'Backordered')
    & (df_unwrapped_salesorders_order_dates['ProductGroup'].isin(get_parts_list()))]
print("Parts in back orders: ", df_unwrapped_salesorders_order_dates_backorder_parts['OrderQuantity'].sum())




print("////////////////////////")
print("Procurement")
print("Purchases")
PurchaseOrders_status_counts = df_PurchaseOrders['OrderStatus'].value_counts()
print("PurchaseOrders")
print(PurchaseOrders_status_counts)



# print("Total orders in backorder: ", len(df_SalesOrders_order_dates.loc[df_SalesOrders_order_dates['OrderStatus'] == 'Backordered']))




