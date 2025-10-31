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
print(admin_api_access_token)

store_url = "https://r-r-store-locations.myshopify.com"

def get_some_shopify_magnum_store_orders_data(start_date, end_date):

    # Replace with your store name and token
    api_version = "2025-01"
    access_token = admin_api_access_token

    url = f"{store_url}/admin/api/{api_version}/orders.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    # Define your date range (ISO 8601 format)
    params = {
        "status": "any",
        "limit": 250,  # max per page
        "created_at_min": start_date,
        "created_at_max": end_date
    }

    response = requests.get(url, headers=headers, params=params)
    orders = response.json().get('orders', [])

    # Normalize into a DataFrame
    df_orders = pd.json_normalize(
        orders,
        sep="_"  # flatten nested keys with underscores
    )

    df_orders['current_subtotal_price'] = pd.to_numeric(df_orders['current_subtotal_price'], errors='coerce').fillna(0)
    # print("Raw full sum: ", df_orders['current_subtotal_price'].sum())

    return df_orders

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

    print(df_orders)

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

def get_shopify_stores_CPOs(start_date, end_date):
    df_orders = get_shopify_magnum_store_orders_data(start_date, end_date)
    df_orders_clean = prep_shopify_magnum_store_data(df_orders)

    orders_kept_cols = ['id', 'created_at', 'location_id', 'line_name', 'title', 'current_quantity', 'Final Price']
    df_in_depth = df_orders_clean[orders_kept_cols].copy()
    file_path = fr"C:\Users\joshu\Documents\Shopify_API\shopify_store_orders_full.xlsx"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df_in_depth.to_excel(file_path, index=False)
    print(f"Excel file written to: {file_path}")

    #Refurbished - Low Rider 2.0, Low Rider 2.0
    # #Copper, Graphite
    # lowrider_graphite_product_code = 'Low Rider BLK-GPH'
    # lowrider_graphite_product_description = 'CPO Low rider 2.0 - Black-Graphite - 48v 15Ah'
    # lowrider_copper_product_code = 'Low Rider - BLK-CPR'
    # lowrider_copper_product_description = 'Low rider 2.0 - Black-Copper - 48v 15Ah'
    low_rider_list = ['(CPO) Low Rider 2.0', 'Low Rider 2.0 (CPO)', 'Refurbished - Low Rider 2.0',
                       'Lowrider CPO, 10 Day Rental, Purchase after', 'Certified Pre-Owned - Low Rider 2.0']
    # df_orders_clean.loc[df_orders_clean['title'].isin(low_rider_list), 'Bike Type'] = 'Low Rider 2.0 CPO'
    #
    # df_orders_clean.loc[df_orders_clean['line_name'].str.contains("graphite", case=False, na=False), 'Product Code'] = lowrider_graphite_product_code
    # df_orders_clean.loc[df_orders_clean['line_name'].str.contains("graphite", case=False,na=False), 'Product Description'] = lowrider_graphite_product_description
    # df_orders_clean.loc[df_orders_clean['line_name'].str.contains("copper", case=False, na=False), 'Product Code'] = lowrider_copper_product_code
    # df_orders_clean.loc[df_orders_clean['line_name'].str.contains("copper", case=False, na=False), 'Product Description'] = lowrider_copper_product_description
    #
    # #Calypso, Black
    # cosmo_black_product_code = 'CPO23150052'
    # cosmo_black_product_description = 'CPO - Cosmo 2.0 T - Black- 48v 15 Ah'
    # cosmo_calypso_product_code = 'CPO23150051'
    # cosmo_calypso_product_description = 'CPO - Cosmo 2.0 T - Calypso - 48v 15 Ah'
    cosmo_list = ['(CPO) Cosmo 2.0 - Torque', '(CPO) Cosmo 2.0 T', 'CPO Cosmo 2.0 T', 'Cosmo 2.0 T (CPO)']
    # df_orders_clean.loc[df_orders_clean['title'].isin(cosmo_list), 'Bike Type'] = 'Cosmo 2.0 T CPO'
    #
    # # Assign Product Code & Description for Black
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("black", case=False, na=False),
    #     'Product Code'
    # ] = cosmo_black_product_code
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("black", case=False, na=False),
    #     'Product Description'
    # ] = cosmo_black_product_description
    # # Assign Product Code & Description for Calypso
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("calypso", case=False, na=False),
    #     'Product Code'
    # ] = cosmo_calypso_product_code
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("calypso", case=False, na=False),
    #     'Product Description'
    # ] = cosmo_calypso_product_description
    #
    # # pathfinder_list = ['(CPO) Pathfinder 500', '(CPO) Pathfinder Torque']
    # # df_orders_clean.loc[df_orders_clean['title'].isin(pathfinder_list), 'Bike Type'] = 'Pathfinder 500/T CPO'
    #
    # #Copper, Graphite
    # cruiser_graphite_product_code = 'Cruiser BLK-GPH'
    # cruiser_graphite_product_description = 'Cruiser 2.0 - Black-Gunmetal - 48v 15Ah'
    # cruiser_copper_product_code = 'Cruiser BLK-CPR'
    # cruiser_copper_product_description = 'Cruiser 2.0 - Black-Copper - 48v 15Ah'
    cruiser_list = ['(CPO) Cruiser 2.0', 'Cruiser 2.0 (CPO)', 'Refurbished - Cruiser 2.0']
    # df_orders_clean.loc[df_orders_clean['title'].isin(cruiser_list), 'Bike Type'] = 'Cruiser 2.0 CPO'
    #
    # # Assign Product Code & Description for Graphite
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("graphite", case=False, na=False),
    #     'Product Code'
    # ] = cruiser_graphite_product_code
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("graphite", case=False, na=False),
    #     'Product Description'
    # ] = cruiser_graphite_product_description
    # # Assign Product Code & Description for Copper
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("copper", case=False, na=False),
    #     'Product Code'
    # ] = cruiser_copper_product_code
    # df_orders_clean.loc[
    #     df_orders_clean['line_name'].str.contains("copper", case=False, na=False),
    #     'Product Description'
    # ] = cruiser_copper_product_description

    # --- Standardize Bike Type ---
    df_orders_clean.loc[df_orders_clean['title'].isin([
        '(CPO) Low Rider 2.0', 'Low Rider 2.0 (CPO)', 'Refurbished - Low Rider 2.0',
        'Lowrider CPO, 10 Day Rental, Purchase after', 'Certified Pre-Owned - Low Rider 2.0'
    ]), 'Bike Type'] = 'Low Rider 2.0 CPO'

    df_orders_clean.loc[df_orders_clean['title'].isin([
        '(CPO) Cosmo 2.0 - Torque', '(CPO) Cosmo 2.0 T', 'CPO Cosmo 2.0 T', 'Cosmo 2.0 T (CPO)'
    ]), 'Bike Type'] = 'Cosmo 2.0 T CPO'

    df_orders_clean.loc[df_orders_clean['title'].isin([
        '(CPO) Cruiser 2.0', 'Cruiser 2.0 (CPO)', 'Refurbished - Cruiser 2.0'
    ]), 'Bike Type'] = 'Cruiser 2.0 CPO'

    # --- Assign Product Code & Description by Bike Type + Color ---

    # Low Rider
    mask_lowrider = df_orders_clean['Bike Type'] == 'Low Rider 2.0 CPO'

    df_orders_clean.loc[
        mask_lowrider & df_orders_clean['line_name'].str.contains("graphite", case=False, na=False),
        'Product Code'
    ] = 'Low Rider BLK-GPH'
    df_orders_clean.loc[
        mask_lowrider & df_orders_clean['line_name'].str.contains("graphite", case=False, na=False),
        'Product Description'
    ] = 'CPO Low rider 2.0 - Black-Graphite - 48v 15Ah'

    df_orders_clean.loc[
        mask_lowrider & df_orders_clean['line_name'].str.contains("copper", case=False, na=False),
        'Product Code'
    ] = 'Low Rider - BLK-CPR'
    df_orders_clean.loc[
        mask_lowrider & df_orders_clean['line_name'].str.contains("copper", case=False, na=False),
        'Product Description'
    ] = 'Low rider 2.0 - Black-Copper - 48v 15Ah'

    # Cosmo
    mask_cosmo = df_orders_clean['Bike Type'] == 'Cosmo 2.0 T CPO'

    df_orders_clean.loc[
        mask_cosmo & df_orders_clean['line_name'].str.contains("black", case=False, na=False),
        'Product Code'
    ] = 'CPO23150052'
    df_orders_clean.loc[
        mask_cosmo & df_orders_clean['line_name'].str.contains("black", case=False, na=False),
        'Product Description'
    ] = 'CPO - Cosmo 2.0 T - Black- 48v 15 Ah'

    df_orders_clean.loc[
        mask_cosmo & df_orders_clean['line_name'].str.contains("calypso", case=False, na=False),
        'Product Code'
    ] = 'CPO23150051'
    df_orders_clean.loc[
        mask_cosmo & df_orders_clean['line_name'].str.contains("calypso", case=False, na=False),
        'Product Description'
    ] = 'CPO - Cosmo 2.0 T - Calypso - 48v 15 Ah'

    # Cruiser
    mask_cruiser = df_orders_clean['Bike Type'] == 'Cruiser 2.0 CPO'

    df_orders_clean.loc[
        mask_cruiser & df_orders_clean['line_name'].str.contains("graphite", case=False, na=False),
        'Product Code'
    ] = 'Cruiser BLK-GPH'
    df_orders_clean.loc[
        mask_cruiser & df_orders_clean['line_name'].str.contains("graphite", case=False, na=False),
        'Product Description'
    ] = 'Cruiser 2.0 - Black-Gunmetal - 48v 15Ah'

    df_orders_clean.loc[
        mask_cruiser & df_orders_clean['line_name'].str.contains("copper", case=False, na=False),
        'Product Code'
    ] = 'Cruiser BLK-CPR'
    df_orders_clean.loc[
        mask_cruiser & df_orders_clean['line_name'].str.contains("copper", case=False, na=False),
        'Product Description'
    ] = 'Cruiser 2.0 - Black-Copper - 48v 15Ah'



    # all CPO products and their many different titles
    cpo_title_list = low_rider_list + cosmo_list + cruiser_list
    df_cpo_orders = df_orders_clean.loc[df_orders_clean['title'].isin(cpo_title_list)]


    df_locations = get_shopify_locations()
    df = df_cpo_orders.merge(df_locations, how='left', left_on='location_id', right_on='location_id',
                             suffixes=('_order', '_location'))


    # convert dates to proper format
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["created_at Year"] = df["created_at"].dt.year
    df["created_at Month"] = df["created_at"].dt.month_name()
    df["created_at MonthNum"] = df["created_at"].dt.month
    df["created_at Quarter"] = df["created_at"].dt.quarter
    df["created_at Quarter"] = "Q" + df["created_at"].dt.quarter.astype(str)
    df["created_at"] = df["created_at"].dt.tz_localize(None)

    df['Customer Type'] = 'Retail'
    df['Product Group'] = 'Bikes'

    # kept_cols = ['id', 'created_at', 'location_id', 'line_name', 'title', 'price', 'current_quantity', 'Total Price',
    #                     'amount', 'Sub - Discounts', 'Order Discount ratio', 'Final Price']
    #
    # locations_kept_cols = ['id', 'name', 'city', 'zip', 'province', 'country']

    essential_cols = ['id', 'name_location', 'created_at', 'line_name', 'title', 'current_quantity',
                      'Final Price', 'Product Group', 'Bike Type', 'Product Code', 'Product Description',
                      'Customer Type', "created_at Quarter", "created_at Year", "created_at Month", "created_at MonthNum", 'city',
                      'province', 'country', 'zip']

    df = df[essential_cols]

    df.rename(columns={'id': 'order_id', 'name_location': 'Location', 'line_name': 'Product line_name', 'title': 'Product Title',
                       'current_quantity': 'quantity',
                       'city': 'City', 'province': 'State', 'country': 'Country', 'zip': 'Zip'}, inplace=True)

    df['Product Code'] = df['Product Code'].fillna('Unknown')
    df['Product Description'] = df['Product Description'].fillna('Unknown')

    return df