import requests
import pandas as pd
import os
from dotenv import load_dotenv

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)
#shopify magnum stores

def configure():
    load_dotenv()

admin_api_access_token = os.getenv("shopify_stores_Magnum_Bikes_Analytics_admin_api_access_token")

store_url = "https://r-r-store-locations.myshopify.com"

def get_shopify_magnum_store_orders_data(start_date, end_date):
    api_version = "2025-01"
    access_token = admin_api_access_token

    url = f"{store_url}/admin/api/{api_version}/orders.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    all_orders = []
    # params = {"limit": 250, "status": "any"}  # 250 is the max per page
    params = {
        "status": "any",
        "limit": 250,  # max per page
        "created_at_min": start_date,
        "created_at_max": end_date
    }

    count = 0
    while url:
        count += 1
        print("working count store orders: ", count)
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_orders.extend(data.get("orders", []))

        # Look for pagination link headers
        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            # Extract the next page URL from the Link header
            parts = link_header.split(",")
            next_url = None
            for p in parts:
                if 'rel="next"' in p:
                    next_url = p[p.find("<")+1:p.find(">")]
            url = next_url
            params = None  # after the first request, params are embedded in the next_url
        else:
            url = None

    # Normalize into a DataFrame
    df_orders = pd.json_normalize(all_orders, sep="_")

    df_orders['current_subtotal_price'] = pd.to_numeric(df_orders['current_subtotal_price'], errors='coerce').fillna(0)
    # print("Raw full sum: ", df_orders['current_subtotal_price'].sum())

    return df_orders


def get_shopify_locations():
    api_version = "2025-01"
    access_token = admin_api_access_token
    #LOCATIONS
    url = f"{store_url}/admin/api/{api_version}/locations.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    locations = response.json()['locations']   # note: key is 'locations', not 'orders'

    # Normalize into a DataFrame
    df_locations = pd.json_normalize(
        locations,
        sep="_"  # flatten nested keys with underscores
    )

    locations_kept_cols = ['id', 'name', 'city', 'zip', 'province', 'country']
    df_locations = df_locations[locations_kept_cols]
    df_locations.rename(columns={'id':'location_id'}, inplace=True)

    return df_locations


def clean_shopify_magnum_store_data(df):

    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df.drop(columns=['line_items'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df[['line_items']].explode('line_items').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['line_items'])
    invoice_lines_expanded.drop(columns=['id'], inplace=True)
    invoice_lines_expanded.rename(columns={'name' : 'line_name'}, inplace=True)
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)

    #untangle discounts
    base_df = final_df.drop(columns=['discount_allocations'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = final_df[['discount_allocations']].explode('discount_allocations').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['discount_allocations'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)

    return final_df

def prep_shopify_magnum_store_data(df_orders):
    df_orders['current_subtotal_price'] = pd.to_numeric(df_orders['current_subtotal_price'], errors='coerce').fillna(0)

    df_orders_clean = clean_shopify_magnum_store_data(df_orders)

    # Convert columns to numeric (coerce errors will turn non-numeric into NaN)
    df_orders_clean['price'] = pd.to_numeric(df_orders_clean['price'], errors='coerce').fillna(0)
    df_orders_clean['amount'] = pd.to_numeric(df_orders_clean['amount'], errors='coerce').fillna(0)
    df_orders_clean['current_quantity'] = pd.to_numeric(df_orders_clean['current_quantity'], errors='coerce').fillna(0)


    # use current_quantity
    #This is how much a sku costs * quantity without discounts
    df_orders_clean['Total Price'] = df_orders_clean['price'] * df_orders_clean['current_quantity']

    #figure out order level discounts

    #figure out sku level discounts, IF SUB - DISCOUNTS NEGATIVE REPLACE WITH ZERO
    df_orders_clean['Sub - Discounts'] = (df_orders_clean['Total Price'] - df_orders_clean[
        'amount']).clip(lower=0) # full price - discounts


    # figure out order level discounts
    df_orders_subtotal = df_orders[['id', 'current_subtotal_price']].copy()
    df_orders_clean_total_price_grouped = df_orders_clean.groupby(['id'])['Sub - Discounts'].sum().reset_index().copy()
    df_order_discounts = df_orders_subtotal.merge(df_orders_clean_total_price_grouped, how='left', on='id')

    df_order_discounts['Order Discount ratio'] = df_order_discounts['current_subtotal_price'] / df_order_discounts['Sub - Discounts']
    # print(df_order_discounts.head(31))
    df_order_discounts = df_order_discounts[['id', 'Order Discount ratio']]
    # #now put order level discount ratio in df_orders_clean
    df_orders_clean = df_orders_clean.merge(df_order_discounts, how='left', on='id')
    df_orders_clean['Final Price'] = df_orders_clean['Sub - Discounts'] * df_orders_clean['Order Discount ratio']
    #
    # Keep only rows where current_quantity is not zero
    df_orders_clean = df_orders_clean[df_orders_clean['current_quantity'] != 0]

    # print("Cleaned final sum: ", df_orders_clean['Final Price'].sum())


    df_orders_clean.rename(columns={
        'price': 'Unit Price (Before Discounts)',
        'Total Price': 'Line Gross (Qty × Unit Price)',
        'amount': 'SKU Discount',
        'Sub - Discounts': 'Line Net (Before Order-Level Discounts)'
    }, inplace=True)

    return df_orders_clean


def get_shopify_products():
    import requests
    import pandas as pd

    api_version = "2025-01"
    access_token = admin_api_access_token

    url = f"{store_url}/admin/api/{api_version}/products.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    all_products = []
    params = {
        "limit": 250  # max per page
    }

    count = 0
    while url:
        count += 1
        print("working count products: ", count)
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        all_products.extend(data.get("products", []))

        # Look for pagination link headers
        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            # Extract the next page URL from the Link header
            parts = link_header.split(",")
            next_url = None
            for p in parts:
                if 'rel="next"' in p:
                    next_url = p[p.find("<") + 1:p.find(">")]
            url = next_url
            params = None  # after the first request, params are embedded in the next_url
        else:
            url = None

    # Normalize into a DataFrame
    df_products = pd.json_normalize(all_products, sep="_")

    return df_products

#PRODUCTS
# df_products = get_shopify_products()


# product_names = ['(CPO) Cosmo 2.0 T - Calypso', '(CPO) Cosmo 2.0 T - Black', 'Low Rider 2.0 (CPO) - Copper / Like New', 'Low Rider 2.0 (CPO) - Graphite / Like New', ]
# product_titles = ['(CPO) Cosmo 2.0 T', '(CPO) Cosmo 2.0 T', 'Low Rider 2.0 (CPO)', 'Low Rider 2.0 (CPO)']

def get_shopify_stores_sales(start_date, end_date):
    df_orders = get_shopify_magnum_store_orders_data(start_date, end_date)
    df_orders_clean = prep_shopify_magnum_store_data(df_orders)
    df = df_orders_clean

    # orders_kept_cols = ['id', 'created_at', 'location_id', 'line_name', 'title', 'current_quantity', 'Final Price']
    # df = df_orders_clean[orders_kept_cols].copy()

    return df