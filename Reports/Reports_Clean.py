from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel, Unleashed_PurchaseOrders_clean_data_parallel
# from Unleashed_Data.Unleashed_Load import *
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from Quickbooks_Data.Quickbooks_Load import Quickbooks_pl_report_clean
import os
import pandas as pd


def Unleashed_PowerBI_SalesOrder_data(start_date, end_date, reload, save_excel=False):

    if reload:
        df = Unleashed_SalesOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload, save_excel=save_excel)

        df['ProductGroup'] = df['ProductGroup'].fillna('No ProductGroup')
        df['ProductGroup'] = df['ProductGroup'].replace('', 'No ProductGroup')

        df['CustomerType'] = df['CustomerType'].fillna('No CustomerType')
        df['CustomerType'] = df['CustomerType'].replace('', 'No CustomerType')

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_TTM_SalesOrders_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")


    else:
        Unleashed_PowerBI_SalesOrder_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_TTM_SalesOrders_data.xlsx"
        df = pd.read_excel(Unleashed_PowerBI_SalesOrder_data_FILENAME)

    return df


def Unleashed_PowerBI_Inventory_data(today_str, reload):
    if reload:
        df = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

        # Do some cleaning for StockOnHand
        df = df[['ProductCode', 'ProductDescription', 'ProductGroupName', 'QtyOnHand', 'AvgCost', 'TotalCost',
                 'LastModifiedOn']]
        df.rename(columns={'ProductGroupName': 'ProductGroup'}, inplace=True)

        df['ProductGroup'] = df['ProductGroup'].fillna('No ProductGroup')
        df['ProductGroup'] = df['ProductGroup'].replace('', 'No ProductGroup')

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

