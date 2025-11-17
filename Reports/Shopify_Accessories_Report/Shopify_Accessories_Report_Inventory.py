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

import requests
import pandas as pd

def get_shopify_inventory_data_old(location_id):

    api_version = "2025-01"
    access_token = admin_api_access_token
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    url = f"{store_url}/admin/api/{api_version}/inventory_levels.json"
    params = {
        "location_ids": location_id,
        "limit": 250
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    inventory_levels = response.json().get("inventory_levels", [])

    df_inventory = pd.json_normalize(inventory_levels, sep="_")
    return df_inventory

import requests
import pandas as pd

def get_shopify_inventory_data(location_id):
    """
    Fetch all inventory levels for a given Shopify location ID,
    handling pagination until all records are retrieved.
    """

    api_version = "2025-01"
    access_token = admin_api_access_token
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    url = f"{store_url}/admin/api/{api_version}/inventory_levels.json"
    params = {
        "location_ids": location_id,
        "limit": 250
    }

    all_inventory = []
    count = 0

    while url:
        count += 1
        print("working count inventory page:", count)

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_inventory.extend(data.get("inventory_levels", []))

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
    df_inventory = pd.json_normalize(all_inventory, sep="_")
    print("Columns: ", df_inventory.columns)
    return df_inventory



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

def clean_shopify_products(df):
    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df.drop(columns=['variants'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df[['variants']].explode('variants').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['variants'])
    invoice_lines_expanded.rename(columns={'title' : 'Specific_sku_type', 'id' : 'sku_id'}, inplace=True)
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)
    return final_df
