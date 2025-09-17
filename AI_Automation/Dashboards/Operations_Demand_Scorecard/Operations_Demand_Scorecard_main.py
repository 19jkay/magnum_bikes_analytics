import pandas as pd
from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from datetime import datetime, timedelta



today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
print("one year ago: ", one_year_ago_str)

start_date = '2025-01-01'
# end_date = today_str
end_date = '2025-04-01' #for this and the reporting do to the first date of next month, so for january do 01-01 to 02-01
# end_date = today_str

reload_data = True
save_excel = False

# df_SalesOrders = Unleashed_SalesOrders_clean_data_parallel(start_date=one_year_ago_str, end_date=today_str, reload=reload_data, save_excel=save_excel)
df_SalesOrders_dates = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date, end_date=end_date)
# df_SalesOrders = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
# df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)
# df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload_data, save_excel=save_excel)

# df_SalesOrders = df_SalesOrders[['CompletedDate', 'OrderStatus']]

status_counts = df_SalesOrders_dates['OrderStatus'].value_counts()
print("num orders: ", len(df_SalesOrders_dates))
print("status counts: ", status_counts)


