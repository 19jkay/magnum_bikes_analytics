import pandas as pd
from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from datetime import datetime, timedelta


# start_date = '2025-01-01'
# end_date = '2025-09-15'

today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')


df_SalesOrders = Unleashed_SalesOrders_clean_data_parallel(start_date=one_year_ago_str, end_date=today_str, reload=True, save_excel=False)

# Get Stock on Hand data
df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

# df_report = df_stockonhand[['ProductGroupName', 'ProductCode', 'ProductDescription', 'QtyOnHand', 'AvgCost']]
df_stockonhand = df_stockonhand[['ProductGroupName', 'ProductCode', 'QtyOnHand', 'AvgCost']]

# df_SalesOrders_grouped = df_SalesOrders.groupby(['ProductCode'])['OrderQuantity'].sum().reset_index()

# merge stockonhand and sales data
# df_report = df_report.merge(df_SalesOrders, on='ProductCode', how='left')

df_report = df_SalesOrders.merge(df_stockonhand, on='ProductCode', how='left')


df_bikes = df_report.loc[df_report['ProductGroupName'] == 'Bikes']
df_bikes['Bike_type'] = df_bikes['ProductDescription'].str.extract(r'^\s*(.*?)\s*-\s*')


IBD_bike_list = ['Wave', 'Bliss', 'Edge']
df_NPI_bikes = df_bikes.loc[df_bikes['Bike_type'].isin(IBD_bike_list)]

df_NPI_bikes = df_NPI_bikes[['Bike_type', 'ProductDescription', 'CompletedDate', 'OrderQuantity']]

# Ensure CompletedDate is in datetime format
df_NPI_bikes['CompletedDate'] = pd.to_datetime(df_NPI_bikes['CompletedDate'], format='%Y-%m-%d')

# Extract year and month into new columns
df_NPI_bikes['Year'] = df_NPI_bikes['CompletedDate'].dt.year
df_NPI_bikes['Month'] = df_NPI_bikes['CompletedDate'].dt.month

# print(df_NPI_bikes)

# df_final = df_NPI_bikes.groupby(['Year', 'Month', 'ProductDescription'])['OrderQuantity'].sum().reset_index()
#
# print(df_final)
# print("/////////////////////////")

df_NPI_bikes_up_to_sept = df_NPI_bikes.loc[df_NPI_bikes['Month'] != 9].copy()
df_sell_through_up_to_sept = df_NPI_bikes_up_to_sept.groupby(['ProductDescription'])['OrderQuantity'].sum().reset_index()
recieved_through_august = [69, 209, 175, 71, 85, 75, 150, 85, 161, 165, 88]
df_sell_through_up_to_sept['Recieved Through August'] = recieved_through_august
df_sell_through_up_to_sept['Sell Through'] = (df_sell_through_up_to_sept['OrderQuantity'] / df_sell_through_up_to_sept['Recieved Through August']) * 100
print(df_sell_through_up_to_sept)

print("/////////////////////////")

df_sell_through = df_NPI_bikes.groupby(['ProductDescription'])['OrderQuantity'].sum().reset_index()
recieved = [144, 265, 250, 145, 85, 75, 150, 138, 217, 245, 128]
df_sell_through['Recieved'] = recieved
df_sell_through['Sell Through'] = (df_sell_through['OrderQuantity'] / df_sell_through['Recieved']) * 100
print(df_sell_through)


print("////////////////////")

df_NPI_bikes_up_to_sept_two = df_NPI_bikes.loc[df_NPI_bikes['Month'] != 9].copy()
monthly_sales = df_NPI_bikes_up_to_sept_two.groupby(['ProductDescription', 'Year', 'Month'])['OrderQuantity'].sum().reset_index()
print(monthly_sales)

print("////////////////////")
avg_monthly_sales = monthly_sales.groupby(['ProductDescription'])['OrderQuantity'].mean().reset_index()
print(avg_monthly_sales)



