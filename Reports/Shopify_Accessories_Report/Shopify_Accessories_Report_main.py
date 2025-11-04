import pandas as pd
import os
from datetime import datetime, timedelta

from Reports.Shopify_Accessories_Report.Shopify_Accessories_Report_Inventory import *
from Reports.Shopify_Accessories_Report.Shopify_Accessories_Report_Sales import *

# start_date = "2025-01-01T00:00:00-00:00"


today = datetime.now().date()   # use UTC date to match the -00:00 offset
end_date = today.strftime("%Y-%m-%dT23:59:59-00:00")

# Date exactly one year ago
one_year_ago = today - timedelta(days=365)
# one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
one_year_ago_str = one_year_ago.strftime("%Y-%m-%dT23:59:59-00:00")
start_date = one_year_ago_str
print(start_date)
print(end_date)


df_orders = get_shopify_stores_sales(start_date, end_date)
df_orders_grouped = (df_orders.groupby(["variant_id", 'location_id'])["current_quantity"].sum().reset_index().rename(columns={"current_quantity": "Units Sold TTM"}))


file_path = fr"C:\Users\joshu\Documents\Reporting\Shopify_Reports\Shopify_raw_orders.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_orders_grouped.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")

#get location data
df_locations = get_shopify_locations()
location_id_list = df_locations['location_id'].to_list()


#get inventory data for each store
inventory_df_list = []
for id in location_id_list:
    df_inventory_one_location = get_shopify_inventory_data(location_id=id)
    inventory_df_list.append(df_inventory_one_location)

df_inventory = pd.concat(inventory_df_list, ignore_index=True)



#get product data
df_products = get_shopify_products()
df_products = clean_shopify_products(df_products)
df_products["SKU Name"] = (
    df_products[["title", 'Specific_sku_type']]
    .astype(str)
    .agg(" ".join, axis=1)
)

file_path = fr"C:\Users\joshu\Documents\Reporting\Shopify_Reports\Shopify_products.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_products.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")

df_orders_grouped = df_orders_grouped.merge(df_products[['SKU Name', 'inventory_item_id', 'sku_id']], how='left', left_on='variant_id', right_on='sku_id')

kept_cols_products = ['SKU Name', 'inventory_item_id', 'title', 'product_type', 'product_type']
df_products = df_products[kept_cols_products]


#combine product, inventory, and location data
df_location_inventory = df_inventory.merge(df_products, how='left', on='inventory_item_id')
df_location_inventory = df_location_inventory.merge(df_locations, how='left', on='location_id')
kept_cols_inventory = ['inventory_item_id', 'SKU Name', 'available', 'name', 'location_id', 'title', 'product_type']
df_location_inventory = df_location_inventory[kept_cols_inventory]
df_location_inventory.rename(columns={'available' : 'Qty Available', 'name' : 'Location'}, inplace=True)

df_location_inventory_sales = df_location_inventory.merge(df_orders_grouped, how='left', on=['inventory_item_id', 'location_id'])


keep_cols_final = ["inventory_item_id", "variant_id", "Location", 'title', 'product_type', "SKU Name_x", "Qty Available", "Units Sold TTM"]
df_location_inventory_sales = df_location_inventory_sales[keep_cols_final]
df_location_inventory_sales.rename(columns={'SKU Name_x' : 'SKU Name Variant', 'title' : 'SKU Shopify', 'Location' : 'Store Location'}, inplace=True)
print(df_location_inventory_sales)
df_location_inventory_sales = df_location_inventory_sales.loc[df_location_inventory_sales['product_type'] == 'Accessories']

file_path = fr"C:\Users\joshu\Documents\Reporting\Shopify_Reports\Shopify_Inventory_Report.xlsx"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
df_location_inventory_sales.to_excel(file_path, index=False)
print(f"Excel file written to: {file_path}")

# 'inventory_item_id' in products
# variant id

#use product variant_id -> id in products to inventory_item_id -> inventory_item_id in inventory report

#put inventory_item_id in df_orders_grouped by merging