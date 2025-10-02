import pandas as pd
from datetime import datetime, timedelta
import os

from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel, Unleashed_stock_adjustment_clean_data_parallel, Unleashed_credit_note_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from AI_Automation.Dashboards.Operations_Demand_Scorecard.Operations_Demand_Scorecard_helper import unwrap_sales_orders, unwrap_warehouse_sales_orders, get_parts_list



today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
# print("one year ago: ", one_year_ago_str)

start_date = '2025-09-22'
end_date = '2025-10-02' #always do one more here than you do in unleashed view sales orders

reload_data = False
save_excel = False


df_stock_adjustment = get_data_parallel(unleashed_data_name='StockAdjustments', start_date=start_date)
#
file_path = fr"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_stock_adjustment_data.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_stock_adjustment.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")
print("DID IT")
#
# df_credit_notes = get_data_parallel(unleashed_data_name='CreditNotes', start_date=start_date, end_date=end_date)
#
# file_path = fr"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_credit_notes_data.xlsx"
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
# df_credit_notes.to_excel(file_path, index=False)
# print(f"Excel file written to: {file_path}")
# print("DID IT")



df_SalesOrders_completed_dates = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
df_SalesOrders_order_dates = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date, end_date=end_date)
print("Fulfuillment")
print("UNLEASHED ORDERS")
Unleashed_Orders = ['New Orders', 'Completed Orders', 'Deleted', 'Completed %', 'Parked', 'Backordered', 'Hold', 'Pre Orders', '# of Units on Pre-Order', 'Accounting, Placed or Ready to Ship']

new_orders = len(df_SalesOrders_order_dates)
completed_orders = df_SalesOrders_completed_dates['OrderNumber'].nunique()
print("New Orders: ", new_orders) #GOT IT
print("Completed Orders: ", completed_orders)

status_counts = df_SalesOrders_order_dates['OrderStatus'].value_counts()
# print(status_counts)
deleted = status_counts.get('Deleted', 0)
completed_percent = completed_orders / new_orders * 100
parked = status_counts.get('Parked', 0)
backordered = status_counts.get('Backordered', 0)
hold = status_counts.get('HOLD', 0)
preorders = 0
num_units_on_preorder = 0

# Sum relevant statuses directly
accounting_placed_ready_to_ship = sum(
    status_counts.get(key, 0)
    for key in ['Accounting', 'Placed', 'Ready to Ship']
)

Unleashed_Orders_data = [new_orders, completed_orders, deleted, completed_percent, parked, backordered, hold, preorders, num_units_on_preorder, accounting_placed_ready_to_ship]

print("Deleted: ", deleted)
print("Completed %: ", completed_percent)
print("Parked: ", parked)
print("Backordered: ", backordered)
print("HOLD: ", hold)
print("Preorders: ", preorders)
print("Number of units on preorder: ", num_units_on_preorder)
print("Accounting Placed Ready to Ship: ", accounting_placed_ready_to_ship)





print("BIKES")
Bikes = ['Total # of Bike Orders: Jam-N', '# of Bikes Fulfilled: Jam-N', 'Jam-N Bikes Per Order', 'Jam-N Service Level(days)']
Bikes_values = []
df_unwrapped_salesorders_order_dates = unwrap_sales_orders(df_SalesOrders_order_dates)
df_unwrapped_salesorders_JamN_bikes_order_dates = df_unwrapped_salesorders_order_dates.loc[
    (df_unwrapped_salesorders_order_dates['WarehouseName'] == 'Jam-N Logistics') &
    (df_unwrapped_salesorders_order_dates['ProductGroup'] == 'Bikes')]

Total_num_Bike_Orders_JamN = df_unwrapped_salesorders_JamN_bikes_order_dates['OrderNumber'].nunique()
print("Total # of Bike Orders: Jam-N: ", Total_num_Bike_Orders_JamN)
Bikes_values.append(Total_num_Bike_Orders_JamN)

df_unwrapped_salesorders_completed_date = unwrap_warehouse_sales_orders(df_SalesOrders_completed_dates)
df_unwrapped_salesorders_completed_date_JamN_bikes = df_unwrapped_salesorders_completed_date.loc[
    (df_unwrapped_salesorders_completed_date['WarehouseName'] == 'Jam-N Logistics') &
    (df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Bikes')]
df_unwrapped_salesorders_completed_date_JamN_bikes = df_unwrapped_salesorders_completed_date_JamN_bikes.loc[df_unwrapped_salesorders_completed_date_JamN_bikes['OrderStatus'] == 'Completed']

num_bikes_fulfilled_JamN = df_unwrapped_salesorders_completed_date_JamN_bikes['OrderQuantity'].sum()
print("Total number of bikes fulfulled JamN 2: ", num_bikes_fulfilled_JamN)
Bikes_values.append(num_bikes_fulfilled_JamN)

JamN_bikes_per_order = num_bikes_fulfilled_JamN / Total_num_Bike_Orders_JamN
print("Jam-N Bikes Per Order: ", JamN_bikes_per_order)
Bikes_values.append(JamN_bikes_per_order)

JamN_service_level_days = 0
print("Jam-N Service Level(days): ", JamN_service_level_days)
Bikes_values.append(JamN_service_level_days)










print("/////////////////////")
print("ACCESSORIES")
Accessories = ['Total Accessories Orders', 'Accessory Units Fulfilled', 'Accessories Per Completed Order', 'Rack Units Fulfilled']
Accessories_values = []
#Now do accessories section
df_unwrapped_salesorders_accessories_order_dates = df_unwrapped_salesorders_order_dates.loc[df_unwrapped_salesorders_order_dates['ProductGroup'] == 'Accessories']
total_accessories_orders = df_unwrapped_salesorders_accessories_order_dates['OrderNumber'].nunique()
Accessories_values.append(total_accessories_orders)
print("Total Accessories Orders: ", total_accessories_orders)

df_unwrapped_salesorders_accessories_completed_date = df_unwrapped_salesorders_completed_date.loc[df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Accessories']
accessory_units_fulfilled = df_unwrapped_salesorders_accessories_completed_date['OrderQuantity'].sum()
Accessories_values.append(accessory_units_fulfilled)
print("Accessory Units Fulfilled: ", accessory_units_fulfilled)

accessories_per_completed_order = accessory_units_fulfilled / total_accessories_orders
Accessories_values.append(accessories_per_completed_order)
print("Accessories Per Completed Order: ", accessories_per_completed_order)

rack_units_fulfilled = 0
Accessories_values.append(rack_units_fulfilled)
print("Rack Units Fulfilled: ", rack_units_fulfilled)




print("////////////////////////")
print("PARTS")
Parts = ['Total Parts Orders', 'Parts Units Fulfilled', 'Parts per Order', 'Acc & Parts Service Level (days)']
Parts_values = []

df_unwrapped_salesorders_parts_order_dates = df_unwrapped_salesorders_order_dates.loc[df_unwrapped_salesorders_order_dates['ProductGroup'].isin(get_parts_list())]
total_parts_orders = df_unwrapped_salesorders_parts_order_dates['OrderNumber'].nunique()
Parts_values.append(total_parts_orders)
print("Total Parts Orders: ", total_parts_orders)

df_unwrapped_salesorders_parts_completed_date = df_unwrapped_salesorders_completed_date.loc[df_unwrapped_salesorders_completed_date['ProductGroup'].isin(get_parts_list())]
parts_units_fulfilled = df_unwrapped_salesorders_parts_completed_date['OrderQuantity'].sum()
Parts_values.append(parts_units_fulfilled)
print("Parts Units Fulfilled: ", parts_units_fulfilled)

parts_per_order = parts_units_fulfilled / total_parts_orders
Parts_values.append(parts_per_order)
print("Parts Per Order: ", parts_per_order)

Acc_Parts_Service_Level_days = 0
Parts_values.append(Acc_Parts_Service_Level_days)
print("Acc & Parts Service Level (days): ", Acc_Parts_Service_Level_days)










print("/////////////////////")
df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)
print("Inventory Management")
print("Inventory Health")
Inventory_Health = ['Bikes in stock', 'Bikes in back orders', 'Parts in stock', 'Parts in back order']
Inventory_Health_values = []

df_stockonhand_bikes = df_stockonhand.loc[df_stockonhand['ProductGroupName'] == 'Bikes']
bikes_in_stock = df_stockonhand_bikes['QtyOnHand'].sum()
Inventory_Health_values.append(bikes_in_stock)
print("Bikes in stock: ", bikes_in_stock)

df_unwrapped_salesorders_order_dates_backorder_bikes = df_unwrapped_salesorders_order_dates.loc[
    (df_unwrapped_salesorders_order_dates['OrderStatus'] == 'Backordered')
    & (df_unwrapped_salesorders_order_dates['ProductGroup'] == 'Bikes')]
bikes_in_back_orders = df_unwrapped_salesorders_order_dates_backorder_bikes['OrderQuantity'].sum()
Inventory_Health_values.append(bikes_in_back_orders)
print("Bikes in back orders: ", bikes_in_back_orders)

df_stockonhand_parts = df_stockonhand.loc[df_stockonhand['ProductGroupName'].isin(get_parts_list())]
parts_in_stock = df_stockonhand_parts['QtyOnHand'].sum()
Inventory_Health_values.append(parts_in_stock)
print("Parts in stock: ", parts_in_stock)

df_unwrapped_salesorders_order_dates_backorder_parts = df_unwrapped_salesorders_order_dates.loc[
    (df_unwrapped_salesorders_order_dates['OrderStatus'] == 'Backordered')
    & (df_unwrapped_salesorders_order_dates['ProductGroup'].isin(get_parts_list()))]
parts_in_back_order = df_unwrapped_salesorders_order_dates_backorder_parts['OrderQuantity'].sum()
Inventory_Health_values.append(parts_in_back_order)
print("Parts in back orders: ", parts_in_back_order)


print("Inventory Control")
Inventory_control = ['Weekly parts sample audit', 'Accuracy %', 'Full Montly Cycle count', 'Count Accuracy']




print("Bike Returns")
Bike_returns = ['Costco Returns', 'Other Returns', 'Total Returns', 'Refurbished', 'Shipped to Jam-N', 'In BY', 'Stripped for Parts', 'Total refurb parts', 'Returns Backlog']
df_stock_adjustment = Unleashed_stock_adjustment_clean_data_parallel(start_date=start_date, reload=True, save_excel=True)
df_credit_notes = Unleashed_credit_note_clean_data_parallel(start_date=start_date, end_date=end_date, reload=True, save_excel=True)

CPO_codes = ['CPO23150052', 'CPO23150051']
df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment.loc[(df_stock_adjustment['ProductCode'].isin(CPO_codes)) & (df_stock_adjustment['Status'] == 'Completed')].copy()
df_stock_adjustment_costco_returns_CPOcosmos_completed['AdjustmentDate'] = pd.to_datetime(df_stock_adjustment_costco_returns_CPOcosmos_completed['AdjustmentDate'])
df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment_costco_returns_CPOcosmos_completed.loc[df_stock_adjustment_costco_returns_CPOcosmos_completed['AdjustmentDate'] < end_date].copy()
costco_returns_cosmo = len(df_stock_adjustment_costco_returns_CPOcosmos_completed)

lowrider_cruiser_product_codes = ['Low Rider BLK-GPH', 'Low Rider - BLK-CPR', 'Cruiser BLK-GPH', 'Cruiser BLK-CPR']
df_credit_notes_costco_returns_lowriders_cruisers = df_credit_notes.loc[(df_credit_notes['ProductCode'].isin(lowrider_cruiser_product_codes)) & (df_credit_notes['Status'] == 'Completed')].copy()

costco_returns_lowriders_cruisers = len(df_credit_notes_costco_returns_lowriders_cruisers)

costco_returns = costco_returns_cosmo + costco_returns_lowriders_cruisers
# Bike_returns_values.append(costco_returns)
print("Cosmo returns: ", costco_returns_cosmo)
print("Costco Returns: ", costco_returns)

other_returns = 0
# Bike_returns_values.append(other_returns)

total_returns = costco_returns + other_returns
# Bike_returns_values.append(total_returns)

refurbished = 0
shipped_to_JamN = 0
in_by = 0
stripped_for_parts = 0
total_refurb_parts = 0
return_backlog = 0

Bike_returns_values = [costco_returns, other_returns, total_returns, refurbished, shipped_to_JamN, in_by, stripped_for_parts, total_refurb_parts, return_backlog]







print("////////////////////////")
df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload_data, save_excel=save_excel)
print("Procurement")
print("Purchases")
Purchases = ['PO placed in process', 'Order Receipt', 'On time delivery']
PurchaseOrders_status_counts = df_PurchaseOrders['OrderStatus'].value_counts()
print("PurchaseOrders")
print(PurchaseOrders_status_counts)


Warranty = ['Warranty Claims Submitted to Factory', 'Warranty Claims Approved', 'Claims recovered within the SLA']




print("/////////////////////////")
print("Product")
Product = ['L3 support tickets Rx', 'L3 Support tickets closed', 'L3 Support backlog', 'T minus milestones', '% hit']




print("////////////////////////")
print("Cust. Service")

Inbound_demand = ['Inbound Calls', 'Tickets', 'Total Inbound Demand']
Zendesk = ['Tickets Generated', 'Tickets Solved', 'Tickets Solved %', 'Warranty Parts Fulfilled', 'Total Outbound Calls', 'Rated Surveys', 'Surveyed satisfaction tickets', '# of Satisfaction Surveys  Completed']
Backlog = ['Open Tickets', 'Back Orders']


# print("Total orders in backorder: ", len(df_SalesOrders_order_dates.loc[df_SalesOrders_order_dates['OrderStatus'] == 'Backordered']))




#Write data to excel
title = 'Operations Scorecard'
global_list = Unleashed_Orders + Bikes + Accessories + Parts + Inventory_Health

date_range = start_date + " to " + end_date
global_values = Unleashed_Orders_data + Bikes_values + Accessories_values + Parts_values + Inventory_Health_values

global_dict = {title : global_list, date_range : global_values}
df_global = pd.DataFrame(global_dict)

file_path = fr"C:\Users\joshu\Documents\Reporting\Unleashed_Reports\Operations_Demand_Scorecard.xlsx"
folder_path = os.path.dirname(file_path)
os.makedirs(folder_path, exist_ok=True)
df_global.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")