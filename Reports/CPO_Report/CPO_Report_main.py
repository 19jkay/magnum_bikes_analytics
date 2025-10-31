from Reports.CPO_Report.CPO_Report_Shopify_Stores import *
from Reports.CPO_Report.CPO_Report_Shopify_Online import *
import pandas as pd
import os
from datetime import datetime


start_date = "2024-01-01T00:00:00-00:00"

# Get today's date
today = datetime.now().date()   # use UTC date to match the -00:00 offset
end_date = today.strftime("%Y-%m-%dT23:59:59-00:00")
print('Today\'s Date: ', end_date)

end_date = "2025-01-01T00:00:00-00:00"

df_shopify_online_cpos = get_shopify_online_CPOs(start_date, end_date)
df_shopify_stores_cpos = get_shopify_stores_CPOs(start_date, end_date)
# df_shopify_online_cpos = get_shopify_online_CPOs(start_date, end_date)

df_full_shopify_cpos = pd.concat(
    [df_shopify_stores_cpos, df_shopify_online_cpos],
    axis=0,
    ignore_index=True  # resets the index so it’s clean
)

df_full_shopify_cpos['Final Price'] = df_full_shopify_cpos['Final Price'].round(2)

# print("Final Price SUM: ", df_full_shopify_cpos['Final Price'].sum())


file_path = fr"C:\Users\joshu\Documents\Shopify_API\shopify_orders_CPOs.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_full_shopify_cpos.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")
