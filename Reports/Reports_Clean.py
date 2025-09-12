from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel, Unleashed_Customers_clean_data_parallel
# from Unleashed_Data.Unleashed_Load import *
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from Quickbooks_Data.Quickbooks_Load import Quickbooks_pl_report_clean
import os
import pandas as pd
import us
from pyzipcode import ZipCodeDatabase





def Unleashed_PowerBI_SalesOrder_data(start_date, end_date, reload, save_excel=False):

    if reload:
        df = Unleashed_SalesOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload, save_excel=save_excel)
        # df_customers = Unleashed_Customers_clean_data_parallel(reload=reload, save_excel=save_excel)

        # df_customers = df_customers.loc[df_customers['AddressType'] == 'Postal']
        # df_customers = df_customers.drop_duplicates(subset='CustomerCode', keep='first')
        # df_customers = df_customers[['CustomerCode', 'City', 'Region', 'Country', 'PostalCode']]
        # print("Num customers: ", len(df_customers))

        zcdb = ZipCodeDatabase()

        def get_state(zip_code):
            try:
                return zcdb[zip_code].state
            except:
                return None  # Handle invalid or missing ZIP codes gracefully

        df['Region'] = df['PostalCode'].apply(get_state)

        df['ProductGroup'] = df['ProductGroup'].fillna('No ProductGroup')
        df['ProductGroup'] = df['ProductGroup'].replace('', 'No ProductGroup')

        df['CustomerType'] = df['CustomerType'].fillna('No CustomerType')
        df['CustomerType'] = df['CustomerType'].replace('', 'No CustomerType')

        # df['CompletedDate'] = pd.to_datetime(df['CompletedDate'])
        df['CompletedDate Year'] = pd.to_datetime(df['CompletedDate']).dt.year
        df['CompletedDate Month'] = pd.to_datetime(df['CompletedDate']).dt.strftime('%B')  # Full month name

        # #merge the dataframes
        # print("Num sales before: ", len(df))
        # df_merged = df.merge(df_customers, on='CustomerCode', how='left')
        # print("Num sales after: ", len(df_merged))

        df_bikes = df.loc[df['ProductGroup'] == 'Bikes'].copy()
        df_bikes['Bike Type'] = df_bikes['ProductDescription'].str.extract(r'^\s*(.*?)\s*-\s*')

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_TTM_SalesOrders_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_TTM_SalesOrders_Bikes_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df_bikes.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")


    else:
        Unleashed_PowerBI_SalesOrder_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_TTM_SalesOrders_data.xlsx"
        df = pd.read_excel(Unleashed_PowerBI_SalesOrder_data_FILENAME)

        Unleashed_PowerBI_SalesOrder_Bikes_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_TTM_SalesOrders_Bikes_data.xlsx"
        df_bikes = pd.read_excel(Unleashed_PowerBI_SalesOrder_Bikes_data_FILENAME)



    return df, df_bikes


def Unleashed_PowerBI_Inventory_data(today_str, reload):
    if reload:
        df = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

        # Do some cleaning for StockOnHand
        df = df[['ProductCode', 'ProductDescription', 'ProductGroupName', 'QtyOnHand', 'AvgCost', 'TotalCost',
                 'LastModifiedOn']]
        df.rename(columns={'ProductGroupName': 'ProductGroup'}, inplace=True)

        df['ProductGroup'] = df['ProductGroup'].fillna('No ProductGroup')
        df['ProductGroup'] = df['ProductGroup'].replace('', 'No ProductGroup')

        # #convert to numeric columns
        # df['QtyOnHand'] = pd.to_numeric(df['QtyOnHand'], errors='coerce')
        # df['OrderQuantity'] = pd.to_numeric(df['OrderQuantity'], errors='coerce')
        # df['OrderQuantity'] = df['OrderQuantity'].fillna(0)
        #
        # # compute WOH
        # df['WOH'] = (df['QtyOnHand'] / df['OrderQuantity']) * 52
        # df['WOH'] = df['WOH'].fillna("NaN")

        #compute total value
        # df['Total Value'] = df['QtyOnHand'] * df['AvgCost']

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_parallel_StockOnHand_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")

    else:
        Unleashed_PowerBI_Inventory_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_parallel_StockOnHand_data.xlsx"
        df = pd.read_excel(Unleashed_PowerBI_Inventory_data_FILENAME)

    return df

def Unleashed_PowerBI_PurchaseOrders_data(start_date, end_date, reload, save_excel=False):
    if reload:
        df = Unleashed_PurchaseOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload, save_excel=save_excel)

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_parallel_PurchaseOrder_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")
    else:
        Unleashed_PowerBI_PurchaseOrder_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_parallel_PurchaseOrder_data.xlsx"
        df = pd.read_excel(Unleashed_PowerBI_PurchaseOrder_data_FILENAME)

    return df


def Quickbooks_PowerBI_PandL_data(start_date, end_date, reload):

    if reload:
        df = Quickbooks_pl_report_clean(start_date, end_date)

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\quickbooks_PandL_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")

    else:
        quickbooks_PowerBI_PandL_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\quickbooks_PandL_data.xlsx"
        df = pd.read_excel(quickbooks_PowerBI_PandL_data_FILENAME)

    return df


def Unleashed_PowerBI_WOH_report(reload):
    from datetime import datetime, timedelta
    if reload:
        # Today's date
        today = datetime.today()
        today_str = today.strftime('%Y-%m-%d')

        # Date exactly one year ago
        one_year_ago = today - timedelta(days=365)
        one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

        # print("Today: ", today_str)
        # print("One year ago: ", one_year_ago_str)

        reload_data = True
        save_excel = False
        # Get TTM (Trailing Twelve Months) data
        df_SalesOrders = Unleashed_SalesOrders_clean_data_parallel(start_date=one_year_ago_str, end_date=today_str, reload=reload_data, save_excel=save_excel)

        # Get Stock on Hand data
        df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

        df_report = df_stockonhand[['ProductGroupName', 'ProductCode', 'ProductDescription', 'QtyOnHand', 'AvgCost']]

        df_SalesOrders_grouped = df_SalesOrders.groupby(['ProductCode'])['OrderQuantity'].sum().reset_index()

        # merge stockonhand and sales data
        df_report = df_report.merge(df_SalesOrders_grouped, on='ProductCode', how='left')

        # Convert 'QtyOnHand' to numeric column
        df_report['QtyOnHand'] = pd.to_numeric(df_report['QtyOnHand'], errors='coerce')

        # fill na with zeros so that blank sales entrees are zeros since there were no sales
        df_report['OrderQuantity'] = pd.to_numeric(df_report['OrderQuantity'], errors='coerce')
        df_report['OrderQuantity'] = df_report['OrderQuantity'].fillna(0)

        # compute WOH
        df_report['WOH'] = (df_report['QtyOnHand'] / df_report['OrderQuantity']) * 52
        df_report['WOH'] = pd.to_numeric(df_report['WOH'], errors='coerce')

        df_report['Total Value'] = df_report['QtyOnHand'] * df_report['AvgCost']

        # replace blanks with No Product Group in 'ProductGroupName'
        df_report['ProductGroupName'] = df_report['ProductGroupName'].replace('', 'No Product Group')

        # rename columns to non-technical names
        df_report.rename(columns={'OrderQuantity': 'Units sold TTM'}, inplace=True)
        df_report.rename(columns={'ProductGroupName': 'Product Group'}, inplace=True)
        df_report.rename(columns={'ProductCode': 'Product Code'}, inplace=True)
        df_report.rename(columns={'ProductDescription': 'Product Description'}, inplace=True)
        df_report.rename(columns={'QtyOnHand': 'Qty On Hand'}, inplace=True)
        df_report.rename(columns={'AvgCost': 'Avg Cost'}, inplace=True)

        # get final
        df_report = df_report[
            ['Product Group', 'Product Code', 'Product Description', 'Qty On Hand', 'Units sold TTM', 'WOH', 'Avg Cost',
             'Total Value']]


        def bucket_woh(woh):
            if woh < 12:
                return "Low Stock (WOH < 12 weeks)"
            elif 12 <= woh and woh < 104:
                return "Healthy Stock (12 <= WOH < 104 weeks)"
            elif woh >= 104:
                return "Overstock (WOH >= 104 weeks)"
            else:
                return "No WOH (No Sales TTM)"

        df_report['WOH Bucket'] = df_report['WOH'].apply(bucket_woh)



        # Try to join purchase data with inventory
        df_PurchaseOrders = Unleashed_PurchaseOrders_clean_data_parallel(start_date='2025-01-10', end_date=today_str, reload=reload_data, save_excel=save_excel)
        df_PurchaseOrders = df_PurchaseOrders.loc[df_PurchaseOrders['OrderStatus'] != 'Complete'].copy()
        df_PurchaseOrders = df_PurchaseOrders.loc[df_PurchaseOrders['OrderQuantity'] != 0].copy()
        df_PurchaseOrders = df_PurchaseOrders[['ProductCode', 'OrderQuantity']]

        df_PurchaseOrders.rename(columns={'OrderQuantity': 'Incoming Product'}, inplace=True)
        df_PurchaseOrders.rename(columns={'ProductCode': 'Product Code'}, inplace=True)

        df_PurchaseOrders_grouped = df_PurchaseOrders.groupby('Product Code')['Incoming Product'].sum()

        df_report = df_report.merge(df_PurchaseOrders_grouped, on='Product Code', how='left')


        print("Len of data: ", len(df_report))

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_WOH_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df_report.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")

    else:
        unleashed_PowerBI_WOH_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_WOH_data.xlsx"
        df_report = pd.read_excel(unleashed_PowerBI_WOH_data_FILENAME)

    return df_report