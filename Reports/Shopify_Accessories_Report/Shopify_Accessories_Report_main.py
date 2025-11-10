import pandas as pd
import os
from datetime import datetime, timedelta, timezone

from Reports.Shopify_Accessories_Report.Shopify_Accessories_Report_Inventory import *
from Reports.Shopify_Accessories_Report.Shopify_Accessories_Report_Sales import *

# start_date = "2025-01-01T00:00:00-00:00"


# today = datetime.now().date()   # use UTC date to match the -00:00 offset
# end_date = today.strftime("%Y-%m-%dT23:59:59-00:00")
#
# # Date exactly one year ago
# one_year_ago = today - timedelta(days=365)
# # one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
# one_year_ago_str = one_year_ago.strftime("%Y-%m-%dT23:59:59-00:00")
# start_date = one_year_ago_str
# print(start_date)
# print(end_date)

#TTM
today = datetime.now(timezone.utc)
end_date = today.strftime("%Y-%m-%dT23:59:59Z")

one_year_ago = today - timedelta(days=365)
start_date = one_year_ago.strftime("%Y-%m-%dT00:00:00Z")
print(start_date)
print(end_date)




#GET ORDERS and TTM sales
df_orders = get_shopify_stores_sales(start_date, end_date)
df_sales_TTM = (df_orders.groupby(["variant_id", 'location_id'])["current_quantity"].sum().reset_index().rename(columns={"current_quantity": "Units Sold TTM"}))

#Get trailing 3 months
df_orders["created_at"] = pd.to_datetime(df_orders["created_at"], utc=True)
three_months_ago = datetime.now(timezone.utc) - timedelta(days=90)
df_orders_T3M = df_orders.loc[df_orders["created_at"] >= three_months_ago] # Filter df_orders to last 3 months
df_sales_T3M = df_orders_T3M.groupby(["variant_id", "location_id"])["current_quantity"].sum().reset_index().rename(columns={"current_quantity": "Units Sold T3M"})

#merge TTM and T3M sales data
df_sales = df_sales_TTM.merge(df_sales_T3M, how="left", on=["variant_id", "location_id"])

# Optional: calculate % of TTM that came from last 3 months
df_sales["Pct_T3M_of_TTM"] = (df_sales["Units Sold T3M"] / df_sales["Units Sold TTM"]).fillna(0)







#GET LOCATIONS
df_locations = get_shopify_locations()
location_id_list = df_locations['location_id'].to_list()



#GET INVENTORY DATA FOR EACH STORE
inventory_df_list = []
for id in location_id_list:
    df_inventory_one_location = get_shopify_inventory_data(location_id=id)
    inventory_df_list.append(df_inventory_one_location)
df_inventory = pd.concat(inventory_df_list, ignore_index=True)



#GET PRODUCTS
df_products = get_shopify_products()
df_products = clean_shopify_products(df_products)
df_products["SKU Name"] = (df_products[["title", 'Specific_sku_type']].astype(str).agg(" ".join, axis=1))
kept_cols_products = ['SKU Name', 'inventory_item_id', 'title', 'product_type', 'sku_id']
df_products = df_products[kept_cols_products]



#Get inventory_item_id attached to TTM sales since this key is needed to join with inventory
df_sales_TTM_with_inventory_item_id = df_sales.merge(df_products[['SKU Name', 'inventory_item_id', 'sku_id']], how='left', left_on='variant_id', right_on='sku_id')






#combine product, inventory, and location data
#Join product, inventory, and location data. This gives inventory repord
df_location_inventory = df_inventory.merge(df_products, how='left', on='inventory_item_id')
df_location_inventory = df_location_inventory.merge(df_locations, how='left', on='location_id')
kept_cols_inventory = ['inventory_item_id', 'SKU Name', 'available', 'name', 'location_id', 'title', 'product_type']
df_location_inventory = df_location_inventory[kept_cols_inventory]
df_location_inventory.rename(columns={'available' : 'Qty Available', 'name' : 'Location'}, inplace=True)






#Join TTM sales to inventory report
df_location_inventory_sales = df_location_inventory.merge(df_sales_TTM_with_inventory_item_id, how='left', on=['inventory_item_id', 'location_id'])
keep_cols_final = ["inventory_item_id", "variant_id", "Location", 'title', 'product_type', "SKU Name_x", "Qty Available", "Units Sold TTM", "Units Sold T3M", "Pct_T3M_of_TTM"]
df_location_inventory_sales = df_location_inventory_sales[keep_cols_final]
df_location_inventory_sales.rename(columns={'SKU Name_x' : 'SKU Name Variant', 'title' : 'SKU Shopify', 'Location' : 'Store Location'}, inplace=True)
df_location_inventory_sales = df_location_inventory_sales.loc[df_location_inventory_sales['product_type'] == 'Accessories']

#Convert 'QtyOnHand' to numeric column
df_location_inventory_sales['Qty Available'] = pd.to_numeric(df_location_inventory_sales['Qty Available'], errors='coerce')
df_location_inventory_sales['Qty Available'] = df_location_inventory_sales['Qty Available'].fillna(0)

# fill na with zeros so that blank sales entrees are zeros since there were no sales
df_location_inventory_sales['Units Sold TTM'] = pd.to_numeric(df_location_inventory_sales['Units Sold TTM'], errors='coerce')
df_location_inventory_sales['Units Sold TTM'] = df_location_inventory_sales['Units Sold TTM'].fillna(0)

df_location_inventory_sales['Units Sold T3M'] = pd.to_numeric(df_location_inventory_sales['Units Sold T3M'], errors='coerce')
df_location_inventory_sales['Units Sold T3M'] = df_location_inventory_sales['Units Sold T3M'].fillna(0)


df_location_inventory_sales['WOH (12 Months)'] = (df_location_inventory_sales['Qty Available']/df_location_inventory_sales['Units Sold TTM']) * 52
df_location_inventory_sales[['WOH (12 Months)']] = df_location_inventory_sales[['WOH (12 Months)']].map(lambda x: 'NaN' if pd.isna(x) else x)

df_location_inventory_sales['WOH (3 Months)'] = (df_location_inventory_sales['Qty Available']/df_location_inventory_sales['Units Sold T3M']) * 13
df_location_inventory_sales[['WOH (3 Months)']] = df_location_inventory_sales[['WOH (3 Months)']].map(lambda x: 'NaN' if pd.isna(x) else x)


file_path = fr"C:\Users\joshu\Documents\Reporting\Shopify_Reports\Shopify_Inventory_Report.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_location_inventory_sales.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")


# file_path = fr"C:\Users\joshu\Documents\Reporting\Shopify_Reports\Shopify_raw_orders.xlsx"
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
# df_sales_TTM.to_excel(file_path, index=False)
# print(f"Excel file written to: {file_path}")
#
# file_path = fr"C:\Users\joshu\Documents\Reporting\Shopify_Reports\Shopify_products.xlsx"
# os.makedirs(os.path.dirname(file_path), exist_ok=True)
# df_products.to_excel(file_path, index=False)
# print(f"Excel file written to: {file_path}")

# 'inventory_item_id' in products
# variant id

#use product variant_id -> id in products to inventory_item_id -> inventory_item_id in inventory report

#put inventory_item_id in df_orders_grouped by merging