import pandas as pd
import os

from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from Unleashed_Data.Unleashed_Helper import convert_ms_date

def unwrap_sales_orders(df):
    customer_df = df['Customer'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['Customer']), customer_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Customer_LastModifiedOn'})

    warehouse_df = df_expanded['Warehouse'].apply(pd.Series)
    df_expanded = pd.concat([df_expanded.drop(columns=['Warehouse']), warehouse_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Warehouse_LastModifiedOn'})
    df_expanded = df_expanded.rename(columns={'Guid': 'Warehouse_Guid'})

    # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
    # Step 1: Create a copy without the 'InvoiceLines' column
    base_df = df_expanded.drop(columns=['SalesOrderLines'])
    # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
    lines_df = df_expanded[['SalesOrderLines']].explode('SalesOrderLines').reset_index()
    # Step 3: Normalize each dictionary into its own row
    invoice_lines_expanded = pd.json_normalize(lines_df['SalesOrderLines'])
    # Step 4: Merge base invoice info with each invoice line
    final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), invoice_lines_expanded], axis=1)
    final_df = final_df.rename(columns={'LastModifiedOn': 'SalesOrderLines_LastModifiedOn'})

    # print(f"Load len after: {len(final_df)}")
    # print("Load Sum of sales after: ", final_df['LineTotal'].sum())

    # convert weird date format to regular date
    final_df['OrderDate'] = final_df['OrderDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['RequiredDate'] = final_df['RequiredDate'].apply(convert_ms_date).dt.date.astype(str)
    final_df['CompletedDate'] = final_df['CompletedDate'].apply(convert_ms_date).dt.date.astype(str)

    final_df.rename(columns={'Product.ProductCode': 'ProductCode'}, inplace=True)
    final_df.rename(columns={'Product.ProductDescription': 'ProductDescription'}, inplace=True)

    df_products = get_data_parallel(unleashed_data_name="Products")
    df_products = df_products[['ProductCode', 'ProductGroup']]

    final_df = final_df.merge(
        df_products[['ProductCode', 'ProductGroup']],
        on='ProductCode',
        how='left'
    )

    final_df['OrderQuantity'] = final_df['OrderQuantity'].astype(float)
    final_df['ProductGroup'] = final_df['ProductGroup'].replace('', 'No Product Group')
    final_df['ProductGroup'] = final_df['ProductGroup'].fillna('No Product Group')

    # file_path = fr"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_unwrap_salesorders_data.xlsx"
    # os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # final_df.to_excel(file_path, index=False)
    # print(f"Excel file written to: {file_path}")
    # print("DID IT")

    return final_df

def unwrap_warehouse_sales_orders(df):
    warehouse_df = df['Warehouse'].apply(pd.Series)
    df_expanded = pd.concat([df.drop(columns=['Warehouse']), warehouse_df], axis=1)
    df_expanded = df_expanded.rename(columns={'LastModifiedOn': 'Warehouse_LastModifiedOn'})
    final_df = df_expanded.rename(columns={'Guid': 'Warehouse_Guid'})

    final_df.rename(columns={'Product.ProductCode': 'ProductCode'}, inplace=True)
    final_df.rename(columns={'Product.ProductDescription': 'ProductDescription'}, inplace=True)

    df_products = get_data_parallel(unleashed_data_name="Products")
    df_products = df_products[['ProductCode', 'ProductGroup']]

    final_df = final_df.merge(
        df_products[['ProductCode', 'ProductGroup']],
        on='ProductCode',
        how='left'
    )

    final_df['OrderQuantity'] = final_df['OrderQuantity'].astype(float)
    final_df['ProductGroup'] = final_df['ProductGroup'].replace('', 'No Product Group')
    final_df['ProductGroup'] = final_df['ProductGroup'].fillna('No Product Group')

    return final_df


def get_parts_list():
    parts_groups = ['Battery',
                    'Bottom Brackets', 'Brakes', 'Chargers',
                    'Cockpit', 'Controllers', 'Conversion Kit', 'Derailleur Hangers',
                    'Displays', 'Drivetrain', 'Electronics', 'Fenders', 'Forks', 'Frame',
                    'Headset', 'Lights', 'Motor Wheels', 'Motors',
                    'Racks', 'Scooters', 'Shifters', 'Throttles', 'Tires', 'Tubes',
                    'Wheels', 'Derailleurs']

    return parts_groups


def clean_sales_orders(df):
    df_products = get_data_parallel(unleashed_data_name="Products")
    df_products = df_products[['ProductCode', 'ProductGroup']]

    df = df.merge(
        df_products[['ProductCode', 'ProductGroup']],
        on='ProductCode',
        how='left'
    )

    df['OrderDate'] = df['OrderDate'].apply(convert_ms_date).dt.date.astype(str)
    df['RequiredDate'] = df['RequiredDate'].apply(convert_ms_date).dt.date.astype(str)
    df['CompletedDate'] = df['CompletedDate'].apply(convert_ms_date).dt.date.astype(str)
    return df