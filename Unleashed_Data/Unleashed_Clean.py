import pandas as pd
from Unleashed_Data.Unleashed_Load import *
from Unleashed_Data.Unleashed_Load_Parralelize import *


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def Unleashed_invoices_clean_data(start_date, end_date, reload=True):

    #rerun API Calls
    if reload:
        df_invoices = get_data(unleashed_data_name="Invoices", start_date=start_date, end_date=end_date)
        df_products = get_data(unleashed_data_name="Products")
        df_customers = get_data(unleashed_data_name="Customers")
    #Pull already loaded API Data
    else:
        INVOICES_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_Invoices_data.xlsx"
        df_invoices = pd.read_excel(INVOICES_FILENAME)

        PRODUCTS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_Products_data.xlsx"
        df_products = pd.read_excel(PRODUCTS_FILENAME)

        CUSTOMERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_Customers_data.xlsx"
        df_customers = pd.read_excel(CUSTOMERS_FILENAME)


    df_products = df_products[['ProductCode', 'ProductGroup']]
    df_customers = df_customers[['CustomerCode', 'CustomerType']]

    df_invoices = df_invoices.loc[df_invoices['InvoiceStatus'] == 'Completed'] #get completed invoices

    df_invoices.rename(columns={'Product.ProductCode': 'ProductCode',
                       'Product.ProductDescription': 'ProductDescription'}, inplace=True)

    df_invoices = df_invoices[['CustomerCode', 'CustomerName', 'InvoiceDate', 'ProductCode', 'ProductDescription', 'OrderQuantity', 'LineTotal']]

    df = df_invoices.merge(
        df_products[['ProductCode', 'ProductGroup']],
        on='ProductCode',
        how='left'
    )

    df = df.merge(
        df_customers[['CustomerCode', 'CustomerType']],
        on='CustomerCode',
        how='left'
    )

    df = df.loc[~df['InvoiceDate'].isna()] #remove nan dates
    df_invoices['LineTotal'] = df_invoices['LineTotal'].astype(float) #convert 'Sub Total' column to float datatype
    # df_invoices['OrderQuantity'] = df_invoices['OrderQuantity'].str.replace(',', '').astype(float)
    df_invoices['OrderQuantity'] = df_invoices['OrderQuantity'].astype(float)
    df_invoices['InvoiceDate'] = pd.to_datetime(df_invoices['InvoiceDate'])
    # df['Year-Month'] = df['Completed Date'].dt.to_period('M')
    # df['Year-Month'] = df['Year-Month'].dt.to_timestamp()
    # df = df.groupby(['Year-Month', 'Product Group', 'Product'])[['Quantity', 'Sub Total']].sum().reset_index()

    file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_clean_Invoices_data.xlsx"
    folder_path = os.path.dirname(file_path)
    os.makedirs(folder_path, exist_ok=True)
    df.to_excel(file_path, index=False)
    print(f"Excel file written to: {file_path}")

    return df


# reload_data = False
# a = Unleashed_invoices_clean_data(start_date='2022-01-01', end_date='2025-06-30', reload=reload_data)