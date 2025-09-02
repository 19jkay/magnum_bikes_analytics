import pandas as pd
from Unleashed_Data.Unleashed_Load_Parralelize import *


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def Unleashed_invoices_clean_data_parallel(start_date, end_date, reload=True):

    #rerun API Calls
    if reload:
        df_invoices = get_data_parallel(unleashed_data_name="Invoices", start_date=start_date, end_date=end_date)
        df_products = get_data_parallel(unleashed_data_name="Products")
        df_customers = get_data_parallel(unleashed_data_name="Customers")
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

    file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_Invoices_data.xlsx"
    folder_path = os.path.dirname(file_path)
    os.makedirs(folder_path, exist_ok=True)
    df.to_excel(file_path, index=False)
    print(f"Excel file written to: {file_path}")

    return df



def Unleashed_SalesOrders_clean_data_parallel(start_date, end_date, reload=True, save_excel=False):

    #rerun API Calls
    if reload:
        df_salesorders = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
        df_products = get_data_parallel(unleashed_data_name="Products")
        df_customers = get_data_parallel(unleashed_data_name="Customers")

        # PRODUCTS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_Products_data.xlsx"
        # df_products = pd.read_excel(PRODUCTS_FILENAME)
        #
        # CUSTOMERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_Customers_data.xlsx"
        # df_customers = pd.read_excel(CUSTOMERS_FILENAME)


        df_products = df_products[['ProductCode', 'ProductGroup']]
        df_customers = df_customers[['CustomerCode', 'CustomerType']]

        df_salesorders = df_salesorders.loc[df_salesorders['OrderStatus'] == 'Completed'] #get completed invoices

        df_salesorders.rename(columns={'Product.ProductCode': 'ProductCode',
                           'Product.ProductDescription': 'ProductDescription'}, inplace=True)

        df_salesorders = df_salesorders[['CustomerCode', 'CustomerName', 'CompletedDate', 'ProductCode', 'ProductDescription', 'OrderQuantity', 'LineTotal']]

        df = df_salesorders.merge(
            df_products[['ProductCode', 'ProductGroup']],
            on='ProductCode',
            how='left'
        )

        df = df.merge(
            df_customers[['CustomerCode', 'CustomerType']],
            on='CustomerCode',
            how='left'
        )

        df = df.loc[~df['CompletedDate'].isna()] #remove nan dates
        df['LineTotal'] = df['LineTotal'].astype(float) #convert 'Sub Total' column to float datatype
        # df_invoices['OrderQuantity'] = df_invoices['OrderQuantity'].str.replace(',', '').astype(float)
        df['OrderQuantity'] = df['OrderQuantity'].astype(float)
        df['ProductGroup'] = df['ProductGroup'].replace('', 'No Product Group')
        df['ProductGroup'] = df['ProductGroup'].fillna('No Product Group')
        # df['CompletedDate'] = pd.to_datetime(df['CompletedDate'])
        # df['Year-Month'] = df['Completed Date'].dt.to_period('M')
        # df['Year-Month'] = df['Year-Month'].dt.to_timestamp()
        # df = df.groupby(['Year-Month', 'Product Group', 'Product'])[['Quantity', 'Sub Total']].sum().reset_index()
        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        SALESORDERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_data.xlsx"
        df = pd.read_excel(SALESORDERS_FILENAME)

    return df


def Unleashed_PurchaseOrders_clean_data_parallel(start_date, end_date, reload=True, save_excel=False):

    #rerun API Calls
    if reload:
        df_purchaseorders = get_data_parallel(unleashed_data_name="PurchaseOrders", start_date=start_date, end_date=end_date)

        df = df_purchaseorders
        df = df[['OrderNumber', 'OrderDate', 'DeliveryDate', 'CompletedDate', 'OrderStatus', 'ProductCode', 'ProductDescription', 'OrderQuantity', 'LineTotal', 'SupplierCode', 'SupplierName']]

        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_PurchaseOrders_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        PURCHASEORDERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_PurchaseOrders_data.xlsx"
        df = pd.read_excel(PURCHASEORDERS_FILENAME)

    return df

def Unleashed_Warehouses_clean_data_parallel(reload=True, save_excel=False):
    if reload:
        df_warehouses = get_data_parallel(unleashed_data_name="Warehouses")

        df = df_warehouses
        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_Warehouse_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        WAREHOUSE_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_Warehouse_data.xlsx"
        df = pd.read_excel(WAREHOUSE_FILENAME)

    return df


def Unleashed_StockOnHand_clean_data_parallel(end_date, reload=True, save_excel=False):
    if reload:
        df_StockOnHand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=end_date)

        df = df_StockOnHand
        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_StockOnHand_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        StockOnHand_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_StockOnHand_data.xlsx"
        df = pd.read_excel(StockOnHand_FILENAME)

    return df



# start_date = '2025-01-04'
# end_date = '2025-09-02'
# a = Unleashed_StockOnHand_clean_data_parallel(end_date=end_date, reload=True, save_excel=True)

