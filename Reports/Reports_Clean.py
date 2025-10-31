from Unleashed_Data.Unleashed_Clean_Parallel import *
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from Quickbooks_Data.Quickbooks_Load import Quickbooks_pl_report_clean
import os
import pandas as pd
import us
from pyzipcode import ZipCodeDatabase




def Unleashed_PowerBI_Invoices_data(start_date, end_date, reload, save_excel=False):
    if reload:
        df_invoices = Unleashed_Invoices_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload, save_excel=save_excel)

        df = df_invoices

        file_path = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_invoices_data.xlsx"
        folder_path = os.path.dirname(file_path)
        os.makedirs(folder_path, exist_ok=True)
        df.to_excel(file_path, index=False)
        print(f"Excel file written to: {file_path}")


    else:
        Unleashed_PowerBI_Invoices_data_FILENAME = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_invoices_data.xlsx"
        df = pd.read_excel(Unleashed_PowerBI_Invoices_data_FILENAME)

    return df


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


def Unleashed_PowerBI_Costco_Returns(start_date, end_date, reload, save_excel=False):
    from datetime import datetime, timedelta
    if reload:

        # df_stock_adjustment = get_data_parallel(unleashed_data_name='StockAdjustments', start_date=start_date)
        df_stock_adjustment = Unleashed_stock_adjustment_clean_data_parallel(start_date=start_date, reload=True, save_excel=True)
        df_credit_notes = Unleashed_credit_note_clean_data_parallel(start_date=start_date, end_date=end_date, reload=True, save_excel=True)

        #cosmo returns stock adjustment
        CPO_codes = ['CPO23150052', 'CPO23150051']
        df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment.loc[
            (df_stock_adjustment['ProductCode'].isin(CPO_codes))
            & (df_stock_adjustment['Status'] == 'Completed')].copy()

        df_stock_adjustment_costco_returns_CPOcosmos_completed['Return Quantity'] = 1
        df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment_costco_returns_CPOcosmos_completed[
            ['AdjustmentDate', 'ProductCode', 'ProductDescription', 'SerialNumber', 'Return Quantity']]
        df_stock_adjustment_costco_returns_CPOcosmos_completed.rename(columns={'AdjustmentDate': 'Date'}, inplace=True)

        #cosmo returns credit note
        cosmo_product_codes = ['23150052', '23150051']
        df_credit_notes_costco_returns_cosmos_completed = df_credit_notes.loc[(df_credit_notes['Status'] == 'Completed')
                                                                              & (df_credit_notes['WarehouseCode'] == 'Costco Returns')
                                                                              & (df_credit_notes['ProductCode'].isin(cosmo_product_codes))].copy()

        df_credit_notes_costco_returns_cosmos_completed['Return Quantity'] = 1
        df_credit_notes_costco_returns_cosmos_completed = df_credit_notes_costco_returns_cosmos_completed[
            ['CreditDate', 'ProductCode', 'ProductDescription', 'SerialNumber', 'Return Quantity']]
        df_credit_notes_costco_returns_cosmos_completed.rename(columns={'CreditDate': 'Date'}, inplace=True)

        df_cosmo_returns = pd.concat(
            [df_stock_adjustment_costco_returns_CPOcosmos_completed, df_credit_notes_costco_returns_cosmos_completed],
            axis=0, ignore_index=True)


def Unleashed_PowerBI_Costco_Returns2(reload):
    from datetime import datetime, timedelta
    if reload:
        today = datetime.today()
        today_str = today.strftime('%Y-%m-%d')

        tomorrow = today + timedelta(days=1)
        tomorrow_str = tomorrow.strftime('%Y-%m-%d')

        # Date exactly one year ago
        one_year_ago = today - timedelta(days=365)
        one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')

        start_date = one_year_ago_str
        end_date = tomorrow_str

        # df_stock_adjustment = get_data_parallel(unleashed_data_name='StockAdjustments', start_date=start_date)
        df_stock_adjustment = Unleashed_stock_adjustment_clean_data_parallel(start_date=start_date, reload=True, save_excel=True)
        df_credit_notes = Unleashed_credit_note_clean_data_parallel(start_date=start_date, end_date=end_date, reload=True, save_excel=True)


        #Cosmos
        CPO_codes = ['CPO23150052', 'CPO23150051']
        # df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment.loc[(df_stock_adjustment['WarehouseCode'] == 'Costco Returns')
        #                                                                   & (df_stock_adjustment['ProductCode'].isin(CPO_codes))
        #                                                                   & (df_stock_adjustment['Status'] == 'Completed')].copy()
        df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment.loc[(df_stock_adjustment['ProductCode'].isin(CPO_codes))
            & (df_stock_adjustment['Status'] == 'Completed')].copy()

        df_stock_adjustment_costco_returns_CPOcosmos_completed['Return Quantity'] = 1
        df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment_costco_returns_CPOcosmos_completed[['AdjustmentDate', 'ProductCode', 'ProductDescription', 'SerialNumber', 'Return Quantity']]
        df_stock_adjustment_costco_returns_CPOcosmos_completed.rename(columns={'AdjustmentDate' : 'Date'}, inplace=True)

        cosmo_product_codes = ['23150052', '23150051']
        df_credit_notes_costco_returns_cosmos_completed = df_credit_notes.loc[(df_credit_notes['Status'] == 'Completed')
                                              & (df_credit_notes['WarehouseCode'] == 'Costco Returns')
                                              & (df_credit_notes['ProductCode'].isin(cosmo_product_codes))].copy()

        df_credit_notes_costco_returns_cosmos_completed['Return Quantity'] = 1
        df_credit_notes_costco_returns_cosmos_completed = df_credit_notes_costco_returns_cosmos_completed[['CreditDate', 'ProductCode', 'ProductDescription', 'SerialNumber', 'Return Quantity']]
        df_credit_notes_costco_returns_cosmos_completed.rename(columns={'CreditDate' : 'Date'}, inplace=True)

        df_cosmo_returns = pd.concat([df_stock_adjustment_costco_returns_CPOcosmos_completed, df_credit_notes_costco_returns_cosmos_completed], axis=0, ignore_index=True)



        thing1 = df_cosmo_returns.groupby('Date', as_index=True)['Return Quantity'].sum()
        print(thing1)
        print("Cosmo Daily AVG: ", thing1.mean())

        #first return is June 9th 2025, there are 79 weekdays between this date and sept 25 2025
        print("Cosmo Adjusted Daily avg: ", thing1.sum() / 79)

        num_duplicates = df_cosmo_returns.duplicated(subset='SerialNumber').sum()
        print("Cosmo Number of duplicate serial numbers:", num_duplicates)

        print("//////////////")
        # Ensure 'Date' is in datetime format
        df_cosmo_returns['Date'] = pd.to_datetime(df_cosmo_returns['Date'])
        # Group by month and sum 'Return Quantity'
        thing1 = df_cosmo_returns.groupby(df_cosmo_returns['Date'].dt.to_period('M'))['Return Quantity'].sum()
        # Optional: convert PeriodIndex back to string for display
        thing1.index = thing1.index.astype(str)
        print(thing1)

        df_cosmo_returns = df_cosmo_returns.drop_duplicates(subset='SerialNumber', keep='first')

        #Low riders and cruisers
        lowrider_cruiser_product_codes = ['Low Rider BLK-GPH', 'Low Rider - BLK-CPR', 'Cruiser BLK-GPH', 'Cruiser BLK-CPR']
        df_credit_notes_costco_returns_lowriders_cruisers = df_credit_notes.loc[(df_credit_notes['ProductCode'].isin(lowrider_cruiser_product_codes))
                                                                                & (df_credit_notes['Status'] == 'Completed')].copy()
        df_credit_notes_costco_returns_lowriders_cruisers['Return Quantity'] = 1
        df_credit_notes_costco_returns_lowriders_cruisers = df_credit_notes_costco_returns_lowriders_cruisers[
            ['CreditDate', 'ProductCode', 'ProductDescription', 'SerialNumber', 'Return Quantity']]

        df_credit_notes_costco_returns_lowriders_cruisers.rename(columns={'CreditDate': 'Date'}, inplace=True)

        thing2 = df_credit_notes_costco_returns_lowriders_cruisers.groupby('Date', as_index=True)['Return Quantity'].sum()
        print(thing2)
        print("Lowriders cruisers Daily AVG: ", thing1.mean())

        # first return is June 9th 2025, there are 79 weekdays between this date and sept 25 2025
        print("Lowriders cruisers Adjusted Daily avg: ", thing1.sum() / 79)

        num_duplicates2 = df_credit_notes_costco_returns_lowriders_cruisers.duplicated(subset='SerialNumber').sum()
        print("Lowriders cruisers Number of duplicate serial numbers:", num_duplicates2)

        # df_costco_returns = pd.concat([df_cosmo_returns, df_credit_notes_costco_returns_lowriders_cruisers], axis=0, ignore_index=True)


        file_path_stock_adjustment = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_costco_returns_cosmo.xlsx"
        folder_path = os.path.dirname(file_path_stock_adjustment)
        os.makedirs(folder_path, exist_ok=True)
        df_cosmo_returns.to_excel(file_path_stock_adjustment, index=False)
        print(f"Excel file written to: {file_path_stock_adjustment}")

        file_path_stock_adjustment = r"C:\Users\joshu\Documents\Reporting\PowerBI_data\unleashed_reports_costco_returns_lowriders_cruisers.xlsx"
        folder_path = os.path.dirname(file_path_stock_adjustment)
        os.makedirs(folder_path, exist_ok=True)
        df_credit_notes_costco_returns_lowriders_cruisers.to_excel(file_path_stock_adjustment, index=False)
        print(f"Excel file written to: {file_path_stock_adjustment}")



    # else:
    #     unleashed_stock_adjustment_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_stock_adjustment_data.xlsx"
    #     df_stock_adjustment = pd.read_excel(unleashed_stock_adjustment_FILENAME)

    return df_stock_adjustment_costco_returns_CPOcosmos_completed, df_credit_notes_costco_returns_cosmos_completed


def clustered_comments():
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
#
#     # Paste the list produced earlier (or load from file). Example variable name: clustered_comments
#     clustered_comments = [
#         {"comment": "NOT CHARGINGC/D", "cluster": "Battery & Charging Issues"},
#         {"comment": "CORD IS NOT LONG ENOUGH", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "TIRES NOT FUNCTIONING", "cluster": "Brake & Tire Issues"},
#         {"comment": "QUIT WORKING", "cluster": "General Dissatisfaction"},
#         {"comment": "ROTTER ON FRONT TIRE BENT", "cluster": "Brake & Tire Issues"},
#         {"comment": "USED", "cluster": "General Dissatisfaction"},
#         {"comment": "TOO FAST", "cluster": "Performance Complaints"},
#         {"comment": "D RAIL IS SLIPPING", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "IT WOULD NOT CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "BRAKES DONT WORK WELL", "cluster": "Brake & Tire Issues"},
#         {"comment": "GOTB THE WRONG ONE", "cluster": "General Dissatisfaction"},
#         {"comment": "PEDAL BROKE", "cluster": "Mechanical Failures"},
#         {"comment": "DERAILER IS OFF LIGHT ISNT BRIGHT", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "DOESN'T WANT", "cluster": "General Dissatisfaction"},
#         {"comment": "USED. TOO HEAVY AND TOO BIG", "cluster": "Too Heavy / Too Big"},
#         {"comment": "BREAKS ARE SQUELING", "cluster": "Brake & Tire Issues"},
#         {"comment": "DOESNT WORK", "cluster": "General Dissatisfaction"},
#         {"comment": "DW", "cluster": "General Dissatisfaction"},
#         {"comment": "BATTERY WILL NOTCHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "DEFECTIVE", "cluster": "General Dissatisfaction"},
#         {"comment": "SEAT KEPT MOVING", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "WIFE SAID NO", "cluster": "General Dissatisfaction"},
#         {"comment": "TOO HEAVY NOTHING WRONG WITH IT", "cluster": "Too Heavy / Too Big"},
#         {"comment": "USED– TOO FAST FOR NEEDS", "cluster": "Performance Complaints"},
#         {"comment": "UDNW/ BATTERY DOES NOT WRK WELL", "cluster": "Battery & Charging Issues"},
#         {"comment": "REAR TIRE BLEW OFF THE BEAD. IT IS FLAT.", "cluster": "Brake & Tire Issues"},
#         {"comment": "BOUGHT TODAY CORD CUT", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "WIRE FOR LIGHT TO SHORT", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "BATTERY DOES NOT CHARGE AT ALL", "cluster": "Battery & Charging Issues"},
#         {"comment": "FOOT EDAL FELL OFF", "cluster": "Mechanical Failures"},
#         {"comment": "PUT TOGETHER WRONG", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "FLATTIRE", "cluster": "Brake & Tire Issues"},
#         {"comment": "TO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "PART OF IT IS NOTWORKING", "cluster": "General Dissatisfaction"},
#         {"comment": "SON DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "BATTER BAD AND SHIFTS GEARS ITSELF ONHIL", "cluster": "Battery & Charging Issues"},
#         {"comment": "WIRE ASSEMBLE UNABLE TO ATTACH BASKET", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "WIFE DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT WORK", "cluster": "General Dissatisfaction"},
#         {"comment": "ONE BOLT IS STRIPPED", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "NO WANT", "cluster": "General Dissatisfaction"},
#         {"comment": "JUST WANTED TO TRY OUT", "cluster": "General Dissatisfaction"},
#         {"comment": "DEJO DE CARGA", "cluster": "Battery & Charging Issues"},
#         {"comment": "BRAKES DO NOT WORK", "cluster": "Brake & Tire Issues"},
#         {"comment": "DIDNT NEED IT NOTHING WRONG", "cluster": "General Dissatisfaction"},
#         {"comment": "DIES QUICK", "cluster": "Battery & Charging Issues"},
#         {"comment": "BENT FRAME", "cluster": "Mechanical Failures"},
#         {"comment": "WIRE PREVENTS STEERING", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "DID NOT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT NEED", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "DOESNT CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DOESNT WORK", "cluster": "General Dissatisfaction"},
#         {"comment": "BACK TIRE HAS LEAK", "cluster": "Brake & Tire Issues"},
#         {"comment": "BAD MAKES ALOT OF NOISE", "cluster": "Performance Complaints"},
#         {"comment": "AIR LEAKS IN TIRES", "cluster": "Brake & Tire Issues"},
#         {"comment": "WAS HARD TO ASSEMBLE", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "BREAKS DONT WORK", "cluster": "Brake & Tire Issues"},
#         {"comment": "PEOPLE GOT HURT ON IT", "cluster": "Performance Complaints"},
#         {"comment": "WAYYY TOO HEAVY AND BIG", "cluster": "Too Heavy / Too Big"},
#         {"comment": "NOT HOLDING A CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "PEDALS FELL OFF", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "NOT WORKING", "cluster": "General Dissatisfaction"},
#         {"comment": "BATTERY WONT HOLD CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "GEARS SLIPPINGCHANGED MIND", "cluster": "Mechanical Failures"},
#         {"comment": "DOESNT THINK THEYLL USE IT", "cluster": "General Dissatisfaction"},
#         {"comment": "SLIGHTLY USED TOO BIG FOR PERSON", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DOESNT CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "WASNT WORKING FOR THEM", "cluster": "General Dissatisfaction"},
#         {"comment": "STRIPPED BIKE PEDAL AREA", "cluster": "Mechanical Failures"},
#         {"comment": "DOESNT WORK", "cluster": "General Dissatisfaction"},
#         {"comment": "OCW USED NOT SATISFIED", "cluster": "General Dissatisfaction"},
#         {"comment": "TO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "OPENED", "cluster": "General Dissatisfaction"},
#         {"comment": "ECOM/TOO BIG", "cluster": "Too Heavy / Too Big"},
#         {"comment": "FULLY CHARGED BUT ELECTRICS WONT WORK", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "BATTERY NOT CHARGIN", "cluster": "Battery & Charging Issues"},
#         {"comment": "WONT HOLD A CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "UNCONFORTABLE", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT WANT", "cluster": "General Dissatisfaction"},
#         {"comment": "NOT POWERFUL ENOUGH", "cluster": "Performance Complaints"},
#         {"comment": "DEFECTIVE", "cluster": "General Dissatisfaction"},
#         {"comment": "TRIED DONT NEED", "cluster": "General Dissatisfaction"},
#         {"comment": "WRONG ONE", "cluster": "General Dissatisfaction"},
#         {"comment": "COULDNT GET TO WORK OTHER ONE DID", "cluster": "General Dissatisfaction"},
#         {"comment": "CHAIN COMES OFF– VERY HEAVY TOO!!!!", "cluster": "Mechanical Failures"},
#         {"comment": "PEDAL CAME OFF", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "DNN", "cluster": "General Dissatisfaction"},
#         {"comment": "CABLE IS WRAPPED WRONG WAY", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "DID NOT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "REALLY HEAVY STARTS TO CLICK WHEN GOING", "cluster": "Mechanical Failures"},
#         {"comment": "DID NOT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "TO BIG", "cluster": "Too Heavy / Too Big"},
#         {"comment": "TOO BIG", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DIDNT NEED", "cluster": "General Dissatisfaction"},
#         {"comment": "BACK WHEEL BENT", "cluster": "Brake & Tire Issues"},
#         {"comment": "PEDAL ASSIST DOES NOT WORK", "cluster": "Battery & Charging Issues"},
#         {"comment": "DIDNT NEED", "cluster": "General Dissatisfaction"},
#         {"comment": "TONS OF ISSUES", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "FOUND BETTER DEAL", "cluster": "General Dissatisfaction"},
#         {"comment": "BIKE LAUNCHES FORWARD UNEXPECTANTLY", "cluster": "Performance Complaints"},
#         {"comment": "NOT FUNCTIONING PROPERLY", "cluster": "General Dissatisfaction"},
#         {"comment": "NEVER CHARGED NEVER WORKED OUT OF BOX", "cluster": "Battery & Charging Issues"},
#         {"comment": "USED", "cluster": "General Dissatisfaction"},
#         {"comment": "BATTERY DONT HOLD CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "DIDN'T NEED – OPENED", "cluster": "General Dissatisfaction"},
#         {"comment": "WANTS ANOTHER ONE", "cluster": "General Dissatisfaction"},
#         {"comment": "MISSING SCREWS", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "TOO BIG", "cluster": "Too Heavy / Too Big"},
#         {"comment": "CHAIN CAME OFF", "cluster": "Mechanical Failures"},
#         {"comment": "USED NOT HOLD CHARGE VERY LONG", "cluster": "Battery & Charging Issues"},
#         {"comment": "OPEN CAME DAMAGE", "cluster": "Mechanical Failures"},
#         {"comment": "MAMKING WERID STOPS", "cluster": "Performance Complaints"},
#         {"comment": "THEY BOUGHT A RETURN BUT IT DID NOT WORK", "cluster": "General Dissatisfaction"},
#         {"comment": "DW", "cluster": "General Dissatisfaction"},
#         {"comment": "TO BIG OPENED TRYED", "cluster": "Too Heavy / Too Big"},
#         {"comment": "TO SHORT FOR IT", "cluster": "Too Heavy / Too Big"},
#         {"comment": "BATTERY GETS TOO HOT", "cluster": "Battery & Charging Issues"},
#         {"comment": "FALLING APART", "cluster": "Mechanical Failures"},
#         {"comment": "MAKE NOISE", "cluster": "Performance Complaints"},
#         {"comment": "HAD A FLAT TIRE", "cluster": "Brake & Tire Issues"},
#         {"comment": "WONT CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "FLAT TIRE", "cluster": "Brake & Tire Issues"},
#         {"comment": "NOT HOLDING CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "TO BIG FOR CHILD", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DIDNT T LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "BATTERY DIDN'T WORK IT DOESN'T TURN ON", "cluster": "Battery & Charging Issues"},
#         {"comment": "BAD", "cluster": "General Dissatisfaction"},
#         {"comment": "HAS AS SHORT IN THE WIRING", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "FRONT PIECE BROKEN", "cluster": "Mechanical Failures"},
#         {"comment": "RECIEVED BOX DAMAGED/ NOT OPENED", "cluster": "Mechanical Failures"},
#         {"comment": "CABLE IS NOTLONG ENOUGH ON LIGHT", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "WONT CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "NO LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "TOO MUCH GOING OUTN", "cluster": "Performance Complaints"},
#         {"comment": "JUST NOT WORTH THE MONEY", "cluster": "General Dissatisfaction"},
#         {"comment": "847 WONT TURN ON", "cluster": "Battery & Charging Issues"},
#         {"comment": "SQEAKING", "cluster": "Performance Complaints"},
#         {"comment": "TOO TALL", "cluster": "Too Heavy / Too Big"},
#         {"comment": "CHAIN BROKE", "cluster": "Mechanical Failures"},
#         {"comment": "DIDNT LIE", "cluster": "General Dissatisfaction"},
#         {"comment": "DINDT WANT", "cluster": "General Dissatisfaction"},
#         {"comment": "NOT VERY FAST DONT LIKE IT", "cluster": "Performance Complaints"},
#         {"comment": "BATTERY LIFE", "cluster": "Battery & Charging Issues"},
#         {"comment": "IT A CRAPPY BIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "KEEPS SHUTTING OFF RANDOMLY", "cluster": "Battery & Charging Issues"},
#         {"comment": "USED DOESNT PEDAL COMES OFF", "cluster": "Mechanical Failures"},
#         {"comment": "HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "TOO BIG FOR KIDS", "cluster": "Too Heavy / Too Big"},
#         {"comment": "WRONG ITEM ADVERTISED", "cluster": "General Dissatisfaction"},
#         {"comment": "BAD BATTERY WONT HOLD CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "WASNT CHARGING / BOUGHT LESS THAN A WEEK", "cluster": "Battery & Charging Issues"},
#         {"comment": "TOO HEAVY TO USE", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "BROKEN", "cluster": "Mechanical Failures"},
#         {"comment": "NO LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "DOES NOT WORK PROPERLY", "cluster": "General Dissatisfaction"},
#         {"comment": "TIRE IS FLAT", "cluster": "Brake & Tire Issues"},
#         {"comment": "SEAT DOESNT STAYUP", "cluster": "Mechanical Failures"},
#         {"comment": "NEVER OPENED DAMAGED BOX", "cluster": "Mechanical Failures"},
#         {"comment": "TOO BIG TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "ELECTRIC PART NOT WORKING", "cluster": "Battery & Charging Issues"},
#         {"comment": "CANT GO UPHILL AS FAST AS DOWNHILL", "cluster": "Performance Complaints"},
#         {"comment": "DIDNT WANT NEED", "cluster": "General Dissatisfaction"},
#         {"comment": "NOT CHARGING", "cluster": "Battery & Charging Issues"},
#         {"comment": "DIDNT LIKE/ USED", "cluster": "General Dissatisfaction"},
#         {"comment": "GOT WRONG COLOR", "cluster": "General Dissatisfaction"},
#         {"comment": "CABLE BROKE ON FRONT", "cluster": "Mechanical Failures"},
#         {"comment": "BREAKS ACTING WEIRD", "cluster": "Brake & Tire Issues"},
#         {"comment": "DOESNT WORK", "cluster": "Battery & Charging Issues"},
#         {"comment": "USED –TOO POWERFUL", "cluster": "Performance Complaints"},
#         {"comment": "BATTERY ISSUE", "cluster": "Battery & Charging Issues"},
#         {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "ECOM– WANTED A DIFERENT ONE", "cluster": "General Dissatisfaction"},
#         {"comment": "NEVER PROPERLY WORKED.", "cluster": "Battery & Charging Issues"},
#         {"comment": "TOO JERKY", "cluster": "Performance Complaints"},
#         {"comment": "CLICKING AND GEARS SLIPPING", "cluster": "Mechanical Failures"},
#         {"comment": "ECOM", "cluster": "General Dissatisfaction"},
#         {"comment": "ECOM", "cluster": "General Dissatisfaction"},
#         {"comment": "MECHANISM WON'T UNBOLT FOR LIGHT", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "ECOMM ARRIVED LATE", "cluster": "General Dissatisfaction"},
#         {"comment": "THE BRAKE BROKE", "cluster": "Brake & Tire Issues"},
#         {"comment": "SOMETIMES WORKS/ ERRRO", "cluster": "Battery & Charging Issues"},
#         {"comment": "NOT FAST ENOUGH", "cluster": "Performance Complaints"},
#         {"comment": "DIDNTWNAT", "cluster": "General Dissatisfaction"},
#         {"comment": "ONLINE...NEW", "cluster": "General Dissatisfaction"},
#         {"comment": "KEEPS STOPPING ON THEM", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "TOO HEAVYY FOR PERSON", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DOES NOTWORK FOR MEMBER", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT WANT", "cluster": "General Dissatisfaction"},
#         {"comment": "NEVER WORKED", "cluster": "Battery & Charging Issues"},
#         {"comment": "DIDNLTIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "STOPPED WORKING", "cluster": "Battery & Charging Issues"},
#         {"comment": "MAKING NOISES", "cluster": "Performance Complaints"},
#         {"comment": "MULTIPLE MALFUNCTIONS", "cluster": "Mechanical Failures"},
#         {"comment": "DOESNT CHARGE OR START UP", "cluster": "Battery & Charging Issues"},
#         {"comment": "NEITHER OF THEM CHARGE OR START UP", "cluster": "Battery & Charging Issues"},
#         {"comment": "SQUEAKING BREAKS", "cluster": "Brake & Tire Issues"},
#         {"comment": "BREAKS SQUEAKS AND BAD GEARS", "cluster": "Brake & Tire Issues"},
#         {"comment": "NOT WORKING", "cluster": "General Dissatisfaction"},
#         {"comment": "IT WON'T START EVEN FULL CHARGED", "cluster": "Battery & Charging Issues"},
#         {"comment": "DOESNT CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "STOPPED WORKING", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT WANT – OK PER AMANDA", "cluster": "General Dissatisfaction"},
#         {"comment": "USED", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "BROKEN", "cluster": "Mechanical Failures"},
#         {"comment": "DNW", "cluster": "General Dissatisfaction"},
#         {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DIDNT LIKE BIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "SEAT BROKE WEAK LITTLE PIECES KEEPBREAK", "cluster": "Mechanical Failures"},
#         {"comment": "USED", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "RETURN BC FELL OFF BIKE", "cluster": "Mechanical Failures"},
#         {"comment": "FRONT TIRE FLAT F & B LIGHTS DON'T WORK", "cluster": "Brake & Tire Issues"},
#         {"comment": "NOT GOOD FOR HOLDING SURFBOARD", "cluster": "General Dissatisfaction"},
#         {"comment": "ELECTRONIC ISSUES", "cluster": "Battery & Charging Issues"},
#         {"comment": "CHANGED MIND", "cluster": "General Dissatisfaction"},
#         {"comment": "BOLT FELL OFF WHILE RIDING", "cluster": "Mechanical Failures"},
#         {"comment": "CHAINS NOT WORKING", "cluster": "Mechanical Failures"},
#         {"comment": "BROKEN", "cluster": "Mechanical Failures"},
#         {"comment": "DAMAGED TIRE", "cluster": "Brake & Tire Issues"},
#         {"comment": "UDW–WILL NOT TAKE ITS CHARGE", "cluster": "Battery & Charging Issues"},
#         {"comment": "WIFE SAID NO", "cluster": "General Dissatisfaction"},
#         {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "TIRE KEEPS BREAKING", "cluster": "Brake & Tire Issues"},
#         {"comment": "DONT WANT RTND OTHER BIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "FRONT TIRE JERKS", "cluster": "Brake & Tire Issues"},
#         {"comment": "WANTS A DIFFERENT BIKE", "cluster": "General Dissatisfaction"},
#         {"comment": "TOO HEAVY FOR MEMBER", "cluster": "Too Heavy / Too Big"},
#         {"comment": "TIRES WENT FLAT ON FIRST USE", "cluster": "Brake & Tire Issues"},
#         {"comment": "DIDTN WANT", "cluster": "General Dissatisfaction"},
#         {"comment": "DOESNT HAVE CHAIN GUARD", "cluster": "Mechanical Failures"},
#         {"comment": "HAS A BREAK INWIRE", "cluster": "Electrical / Assembly Problems"},
#         {"comment": "KICK STAND BROKE OFF", "cluster": "Mechanical Failures"},
#         {"comment": "BACK RIM DAMAGED WHEN SHIPPED", "cluster": "Mechanical Failures"},
#         {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
#         {"comment": "DOES NOT WORK", "cluster": "General Dissatisfaction"},
#         {"comment": "NOT SHIFTING CORRECTLY", "cluster": "Mechanical Failures"}
#     ]
    data = [
        {'comment': 'not chargingc/d', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'CORD IS NOT LONG ENOUGH', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'tires not functioning', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'QUIT WORKING', 'cluster': 'General Dissatisfaction'},
        {'comment': 'rotter on front tire bent', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'USED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'It would not charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'd rail is slipping', 'cluster': 'Mechanical Failures'},
        {'comment': 'TOO FAST', 'cluster': 'Performance Complaints'},
        {'comment': 'brakes dont work well', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'derailer is off  light isnt bright', 'cluster': 'Mechanical Failures'},
        {'comment': 'pedal broke', 'cluster': 'Mechanical Failures'},
        {'comment': 'gotb the wrong one', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesn\'t want', 'cluster': 'General Dissatisfaction'},
        {'comment': 'Used. Too heavy and too big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'breaks are squeling', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'BATTERY WILL NOTCHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'dw', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DOESNT WORK', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DEFECTIVE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'seat kept moving', 'cluster': 'Mechanical Failures'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wife said no', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too heavy nothing wrong with it', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'USED- TOO FAST FOR NEEDS', 'cluster': 'General Dissatisfaction'},
        {'comment': 'WIRE FOR LIGHT TO SHORT', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'bought today cord cut', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'rear tire blew off the bead. it is flat.', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'udnw/ battery does not wrk well', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'Battery does not charge at all', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'PUT TOGETHER WRONG', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'foot [edal fell off', 'cluster': 'Mechanical Failures'},
        {'comment': 'TO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'FLATTIRE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'part of it is notworking', 'cluster': 'General Dissatisfaction'},
        {'comment': 'SON DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'batter bad and shifts gears itself onhil', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'wife didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wire assemble  unable to attach basket', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'one bolt is stripped', 'cluster': 'Mechanical Failures'},
        {'comment': 'DIDNT WORK', 'cluster': 'General Dissatisfaction'},
        {'comment': 'just wanted to try out', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NO WANT', 'cluster': 'General Dissatisfaction'},
        {'comment': '847', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'dejo de carga', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'brakes do not work', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'bent frame', 'cluster': 'Mechanical Failures'},
        {'comment': 'wire prevents steering', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'didnt need it nothing wrong', 'cluster': 'General Dissatisfaction'},
        {'comment': 'dies quick', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'did not like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'back tire has leak', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'TOO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'bad makes alot of noise', 'cluster': 'Performance Complaints'},
        {'comment': 'DOESNT CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'doesnt work', 'cluster': 'General Dissatisfaction'},
        {'comment': 'air leaks in tires', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'was hard to assemble', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'BREAKS DONT WORK', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'people got hurt on it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'not holding a charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'wayyy too heavy and big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'pedals fell off', 'cluster': 'Mechanical Failures'},
        {'comment': 'HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'NOT WORKING', 'cluster': 'General Dissatisfaction'},
        {'comment': 'BATTERY WONT HOLD CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'doesnt think theyll use it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'slightly used too big for person', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'gears slippingchanged mind', 'cluster': 'Mechanical Failures'},
        {'comment': 'wasnt working for them', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesnt charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'ocw used not  satisfied', 'cluster': 'General Dissatisfaction'},
        {'comment': 'stripped bike pedal area', 'cluster': 'Mechanical Failures'},
        {'comment': 'doesnt work', 'cluster': 'General Dissatisfaction'},
        {'comment': 'ECOM/TOO BIG', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'TO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'fully charged but electrics wont work', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'OPENED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'battery not chargin', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'too heavy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'unconfortable', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wont hold a charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'DIDNT WANT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'not powerful enough', 'cluster': 'Performance Complaints'},
        {'comment': 'tried dont need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wrong one', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DEFECTIVE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'couldnt get to work  other one did', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'PEDAL CAME OFF', 'cluster': 'Mechanical Failures'},
        {'comment': 'DNN', 'cluster': 'General Dissatisfaction'},
        {'comment': 'chain comes off- very heavy too!!!!', 'cluster': 'Mechanical Failures'},
        {'comment': 'CABLE IS WRAPPED WRONG WAY', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'really heavy  starts to click when going', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'did not like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DID NOT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'to big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'too big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'BACK WHEEL BENT', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'TONS OF ISSUES', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT NEED', 'cluster': 'General Dissatisfaction'},
        {'comment': '1', 'cluster': 'General Dissatisfaction'},
        {'comment': 'pedal assist does not work', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'found better deal', 'cluster': 'General Dissatisfaction'},
        {'comment': 'never charged never worked out of box', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'battery dont hold charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'bike launches forward unexpectantly', 'cluster': 'Performance Complaints'},
        {'comment': 'USED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NOT FUNCTIONING PROPERLY', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wants another one', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'MISSING SCREWS', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DIDN\'T NEED - OPENED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'mamking werid stops', 'cluster': 'Performance Complaints'},
        {'comment': 'OPEN CAME DAMAGE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'used not hold charge very long', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'chain came off', 'cluster': 'Mechanical Failures'},
        {'comment': 'dw', 'cluster': 'General Dissatisfaction'},
        {'comment': 'to short for it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'battery gets too hot', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'THEY BOUGHT A RETURN BUT IT DID NOT WORK', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TO BIG OPENED TRYED', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'FALLING APART', 'cluster': 'Mechanical Failures'},
        {'comment': 'wont charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'make noise', 'cluster': 'Performance Complaints'},
        {'comment': 'had a flat tire', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'didnt t like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'to big for child', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'NOT HOLDING CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'FLAT TIRE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'FRONT PIECE BROKEN', 'cluster': 'Mechanical Failures'},
        {'comment': 'BAD', 'cluster': 'General Dissatisfaction'},
        {'comment': 'RECIEVED BOX DAMAGED/ NOT OPENED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'HAS AS SHORT IN THE WIRING', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'cable is notlong enough on light', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'wont charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'BATTERY DIDN\'T WORK IT DOESN\'T TURN ON', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'JUST NOT WORTH THE MONEY', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TOO MUCH GOING OUTN', 'cluster': 'General Dissatisfaction'},
        {'comment': '847 WONT TURN ON', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'no like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt lie', 'cluster': 'General Dissatisfaction'},
        {'comment': 'SQEAKING', 'cluster': 'Performance Complaints'},
        {'comment': 'DINDT WANT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too tall', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'chain broke', 'cluster': 'Mechanical Failures'},
        {'comment': 'keeps shutting off randomly', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'not very fast dont like it', 'cluster': 'Performance Complaints'},
        {'comment': 'BATTERY LIFE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'it a crappy bike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too big for kids', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'used doesnt pedal comes off', 'cluster': 'Mechanical Failures'},
        {'comment': 'heavy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'bad battery wont hold charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'wasnt charging / bought less than a week', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'WRONG ITEM ADVERTISED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'broken', 'cluster': 'General Dissatisfaction'},
        {'comment': 'no like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too heavy to use', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'does not work properly', 'cluster': 'General Dissatisfaction'},
        {'comment': 'seat doesnt stayup', 'cluster': 'Mechanical Failures'},
        {'comment': 'TIRE IS FLAT', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'TOO BIG TOO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'electric part not working', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'NEVER OPENED  damaged box', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NOT CHARGING', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'didnt want need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'cant go uphill as fast as downhill', 'cluster': 'Performance Complaints'},
        {'comment': 'CABLE BROKE ON FRONT', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'Got wrong color', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE/ USED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'used -too powerful', 'cluster': 'Performance Complaints'},
        {'comment': 'BREAKS ACTING WEIRD', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'too jerky', 'cluster': 'Performance Complaints'},
        {'comment': 'ECOM- WANTED A DIFERENT ONE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesnt work', 'cluster': 'General Dissatisfaction'},
        {'comment': 'Never properly worked.', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TOO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'BATTERY ISSUE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'clicking and gears slipping', 'cluster': 'Mechanical Failures'},
        {'comment': 'ecomm arrived late', 'cluster': 'General Dissatisfaction'},
        {'comment': 'MECHANISM WON\'T UNBOLT FOR LIGHT', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'ECOM', 'cluster': 'General Dissatisfaction'},
        {'comment': 'ECOM', 'cluster': 'General Dissatisfaction'},
        {'comment': 'the brake broke', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'SOMETIMES WORKS/ ERRRO', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NOT FAST ENOUGH', 'cluster': 'Performance Complaints'},
        {'comment': 'DIDNTWNAT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'online...new', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'KEEPS STOPPING ON THEM', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'never worked', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DOES NOTWORK FOR MEMBER', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt want', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too heavyy for person', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'multiple malfunctions', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesnt charge or start up', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'neither of them charge or start up', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'didnltike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'MAKING NOISES', 'cluster': 'Performance Complaints'},
        {'comment': 'stopped working', 'cluster': 'General Dissatisfaction'},
        {'comment': 'squeaking breaks', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'breaks squeaks and bad gears', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'not working', 'cluster': 'General Dissatisfaction'},
        {'comment': 'IT WON\'T START  EVEN FULL CHARGED', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'stopped working', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DOESNT CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'didnt want - ok per AMANDA', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DNW', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TOO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'BROKEN', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'used', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like bike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'seat broke  weak little pieces keepbreak', 'cluster': 'Mechanical Failures'},
        {'comment': 'FRONT TIRE FLAT  F & B LIGHTS DON\'T WORK', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'RETURN BC FELL OFF BIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'used', 'cluster': 'General Dissatisfaction'},
        {'comment': 'changed mind', 'cluster': 'General Dissatisfaction'},
        {'comment': 'bolt fell off while riding', 'cluster': 'Mechanical Failures'},
        {'comment': 'Electronic Issues', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'not good for holding surfboard', 'cluster': 'General Dissatisfaction'},
        {'comment': 'CHAINS NOT WORKING', 'cluster': 'Mechanical Failures'},
        {'comment': 'BROKEN', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DAMAGED TIRE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'wife said no', 'cluster': 'General Dissatisfaction'},
        {'comment': 'UDW-WILL NOT TAKE ITS CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wants a different bike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'tire keeps breaking', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'dont want rtnd other bike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'front tire jerks', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'TIRES WENT FLAT ON FIRST USE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'DIDTN WANT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too heavy for member', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'has a break inwire', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DOESNT HAVE CHAIN GUARD', 'cluster': 'Mechanical Failures'},
        {'comment': 'BACK RIM DAMAGED WHEN SHIPPED', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'kick stand broke off', 'cluster': 'Mechanical Failures'},
        {'comment': 'too heavy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'too heavy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'NOT SHIFTING CORRECTLY', 'cluster': 'Mechanical Failures'},
        {'comment': 'DOES NOT WORK', 'cluster': 'General Dissatisfaction'},
        {'comment': 'rim is bad', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'USED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'udw', 'cluster': 'General Dissatisfaction'},
        {'comment': 'Did not need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'kick stand broke at bolt', 'cluster': 'Mechanical Failures'},
        {'comment': 'not working like it should', 'cluster': 'General Dissatisfaction'},
        {'comment': 'SCREEN DOESN\'T TURN ON', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'BAD BATTERY', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'DIDNT   LIKE IT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TOO LARGE', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'bought flat tire-put air but still flat', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'WAS TO BIG FOR THEM', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'NOT WORKING', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NO LIGHT', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'used didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TOO SLOW', 'cluster': 'Performance Complaints'},
        {'comment': 'turns off', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'tires keep losing air udw', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'too big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'came in blue ordered black', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'flat tire', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'key don\'t lock', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'REAR LIGHT DOES NOT WORK', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': '305353048000 stopcharching', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'stop charching', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'too dangerous', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesn\'t go as fast as stated', 'cluster': 'Performance Complaints'},
        {'comment': 'THE FRONT TIRE IS DEFECTIVE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'does not work / wont turn on', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too fast', 'cluster': 'Performance Complaints'},
        {'comment': 'don\'t like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE IT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'never opened', 'cluster': 'General Dissatisfaction'},
        {'comment': 'BRAKES ARE MAKING TOO MUCH NOISE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'DAMAGED PRIOR TO PURCHASE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'CHRGE HRRBLE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'ecom - screws less  box open on ship', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'did not liked', 'cluster': 'General Dissatisfaction'},
        {'comment': 'some pedals were stripped', 'cluster': 'Mechanical Failures'},
        {'comment': 'stopped running and brake wont work', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'doesnt   work', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesnt  work properly', 'cluster': 'General Dissatisfaction'},
        {'comment': 'UDW', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too heavvy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'USED TOO BIG', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'BATTERY NOTWOKRIGN', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'JUST DIDNT WANT NOTHING WRONG', 'cluster': 'General Dissatisfaction'},
        {'comment': 'fender doesnt fit', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DINDT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wife dont like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE WONT CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'too heavy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'came damaged', 'cluster': 'General Dissatisfaction'},
        {'comment': 'POOR TIRE QUALITY', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'BAD', 'cluster': 'General Dissatisfaction'},
        {'comment': 'damaged in shipping screen cracked', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'will not hold a charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'cant ride anymore', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DONT LIKE BIKE HAS FLAT', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'dont use', 'cluster': 'General Dissatisfaction'},
        {'comment': 'tire wont hold air', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'to tall for her neck pain', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'did not like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'does not like - too slow', 'cluster': 'Performance Complaints'},
        {'comment': 'stop working', 'cluster': 'General Dissatisfaction'},
        {'comment': 'STOPPED WORKING', 'cluster': 'General Dissatisfaction'},
        {'comment': 'no want flat tire', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'ERROR 4 CODE', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'range not good', 'cluster': 'Performance Complaints'},
        {'comment': 'battery drained too quickly', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'CHANCEHIS MIND', 'cluster': 'General Dissatisfaction'},
        {'comment': 'CANT RIDE IT WITH A CANE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NOT HAPPY', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didn\'t like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'wrong height', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'wont power', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DIDNT NEED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'VERY POOR QAULITY', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TEAL', 'cluster': 'General Dissatisfaction'},
        {'comment': 'kick stand fell off  light doesntworkcd', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'motor notstrongenough', 'cluster': 'Performance Complaints'},
        {'comment': 'bad isnt good quality/falling apart', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like  tires kept popping c/d', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'bad rusted', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DNL', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TO TALL', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'cable broke', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'THE USB PORT IS TOO HARD TO CONNECT', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'not workingatall', 'cluster': 'General Dissatisfaction'},
        {'comment': 'TOO SMALL THROTTLE IS WIERD NOT SMOTH', 'cluster': 'Performance Complaints'},
        {'comment': 'TOO MANY ISSUES', 'cluster': 'General Dissatisfaction'},
        {'comment': 'back tire blew out', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'cable broke ap by paola', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'battery drains quickly', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'not holding charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'BREAK DOESNT WORK AND HORN', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'battery dont hold', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'FLAT TIRE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'wasn\'t holding a charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'PEDDLE BROKEN', 'cluster': 'Mechanical Failures'},
        {'comment': 'hard to put together', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'hands would go numb', 'cluster': 'General Dissatisfaction'},
        {'comment': 'hands going numb', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too big', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'ON LINE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'bad brakes', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'MILES ON BIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NO KICKSTAND', 'cluster': 'Mechanical Failures'},
        {'comment': 'couldn\'t ride it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'shuts down all the time', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'didn\'t like ride', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesnt hold the charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'UDI-TOO HEAVY', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'battery not working', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'CHANGE MIND', 'cluster': 'General Dissatisfaction'},
        {'comment': 'FLAT TIRE', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'broken', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'tried didnt need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'NOT LONGER CHARGES', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'problem with power ooutput', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'used', 'cluster': 'General Dissatisfaction'},
        {'comment': 'cant control throttle', 'cluster': 'Performance Complaints'},
        {'comment': 'WONT TURN ON EVEN AFTER FULL CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'found something else', 'cluster': 'General Dissatisfaction'},
        {'comment': 'GOT A FLAT TIRE AND CANT FIGURE OUT', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'having problems with it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT NEED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DOESN\'T HOLD CHARGE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'changed mind', 'cluster': 'General Dissatisfaction'},
        {'comment': 'kick stand broke', 'cluster': 'Mechanical Failures'},
        {'comment': 'did not finish installing it', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'It says up to 50 miles goes 10 miles', 'cluster': 'Performance Complaints'},
        {'comment': 'too long to charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'doesnt work in cold very well', 'cluster': 'Performance Complaints'},
        {'comment': 'preference issue', 'cluster': 'General Dissatisfaction'},
        {'comment': 'not good', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT WANT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'had miles already', 'cluster': 'General Dissatisfaction'},
        {'comment': 'lcd screen doesnt light up', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'tire blew out', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'not comfortable', 'cluster': 'General Dissatisfaction'},
        {'comment': 'want trun on', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DONT WANT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'GOT A CAR  DONT NEED ANYMORE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'had battery issues', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'BATTERY DOES NOT LAST VERY LONG', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'had too many issues w bike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'hadtoo many issueswithbike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'WASNT AS EXPECTED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'does not charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'STOPS WORKING AFTER AWHILE', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DONT  DO  HILLSWELL', 'cluster': 'Performance Complaints'},
        {'comment': 'DONT DO  HILLS  WELL', 'cluster': 'Performance Complaints'},
        {'comment': 'they changed their mind', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'tired popped', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'not holding bat', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'better price', 'cluster': 'General Dissatisfaction'},
        {'comment': 'Light/chain didn\'t work properly', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'RODE DNL SPEED', 'cluster': 'Performance Complaints'},
        {'comment': 'WIFE DIDNT LIKE IT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'they doent want', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too big and heavy', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'WIFE DIDN\'T LIKE COLOR', 'cluster': 'General Dissatisfaction'},
        {'comment': 'WHEEL WONT TURN', 'cluster': 'Mechanical Failures'},
        {'comment': 'LACKING SECURITY FEATURES  UNSAT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'pieces come off while riding', 'cluster': 'Mechanical Failures'},
        {'comment': 'ecomm/had accident', 'cluster': 'General Dissatisfaction'},
        {'comment': 'STOPPED CHARGING', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'NOT  POWERFUL TO GO UP HILLS', 'cluster': 'Performance Complaints'},
        {'comment': 'BETTER PRICE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'udnw', 'cluster': 'General Dissatisfaction'},
        {'comment': 'back tire issues', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'USED', 'cluster': 'General Dissatisfaction'},
        {'comment': 'never rode it', 'cluster': 'General Dissatisfaction'},
        {'comment': 'cable too short  can\'t turn properly', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'defective', 'cluster': 'General Dissatisfaction'},
        {'comment': 'ECOM', 'cluster': 'General Dissatisfaction'},
        {'comment': 'HIT A POTHOLE AND WONT WORK RIGHT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'THE BATERYANDCHARGER BURNED/ OLIVIAAGREE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'has short/ did not like', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'd wheel keep rubbing', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'TAIL LIGHT NOT WORKING', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'flat tire', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'chain keeps falling off', 'cluster': 'Mechanical Failures'},
        {'comment': 'UDW', 'cluster': 'General Dissatisfaction'},
        {'comment': 'used unable to use', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DAMAGED IN SHIPPING', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DISPLAY NOT COMMING ON', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'didnt go up hills', 'cluster': 'Performance Complaints'},
        {'comment': 'wouldnt get up hills', 'cluster': 'Performance Complaints'},
        {'comment': 'dw', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DIG SCREEN IS FOGGY  HARD TO SEE', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'key wont stay in to activate to go', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'changed mind', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt like the pedal', 'cluster': 'General Dissatisfaction'},
        {'comment': 'battery was not good', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'battery stopped working', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'too tall for mem', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'mem unsat just too tall', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'HANDLE BARS IS STRIPPED', 'cluster': 'Mechanical Failures'},
        {'comment': 'tire blew', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'didnt charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'PUNCTURED', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'SUSPENSION & BREAK AREBROKEN', 'cluster': 'Mechanical Failures'},
        {'comment': 'p[p[', 'cluster': 'General Dissatisfaction'},
        {'comment': 'dont like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too tall and fast', 'cluster': 'Performance Complaints'},
        {'comment': 'BROKEN SEVERAL PLACES', 'cluster': 'Mechanical Failures'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didntlike', 'cluster': 'General Dissatisfaction'},
        {'comment': 'after traveling would not work', 'cluster': 'General Dissatisfaction'},
        {'comment': 'BACK TIRE KEEPS POPPING', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'didnt like', 'cluster': 'General Dissatisfaction'},
        {'comment': 'changed mind', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DID NOT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'motor not working', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'ISSUES WITH TIRES', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'DEFECTIVE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'ecom and came damaged', 'cluster': 'General Dissatisfaction'},
        {'comment': 'battery really bad', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'DOESNT PROFORM', 'cluster': 'Performance Complaints'},
        {'comment': 'broken/chain/used', 'cluster': 'Mechanical Failures'},
        {'comment': 'wont hold charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'poor quality rusting', 'cluster': 'General Dissatisfaction'},
        {'comment': 'too fast', 'cluster': 'Performance Complaints'},
        {'comment': 'HAS GOT A MIND OF IT\'S OWN', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'LIKE NEW', 'cluster': 'General Dissatisfaction'},
        {'comment': 'not a good fit', 'cluster': 'General Dissatisfaction'},
        {'comment': 'GOT A BETTER PRICE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'battery stopped working', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'ISSUE WITH BREAKS', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'chain issues. keeps falling off', 'cluster': 'Mechanical Failures'},
        {'comment': 'ONLINE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'error codes - tire popped', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'KEY DOESNT WORK TO RELEASE BATTERY', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'fell off/broke arm', 'cluster': 'General Dissatisfaction'},
        {'comment': 'broken light', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'DIDNT LIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'didnt want this one', 'cluster': 'General Dissatisfaction'},
        {'comment': 'does not go as fast as ity suppossed to', 'cluster': 'Performance Complaints'},
        {'comment': 'does not charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'too short', 'cluster': 'Too Heavy / Too Big'},
        {'comment': 'it does not hold a charge', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'DOESNT CHARGE  WEIRD NOISE', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'does not connect well to the neck', 'cluster': 'Electrical / Assembly Problems'},
        {'comment': 'ddnt liked', 'cluster': 'General Dissatisfaction'},
        {'comment': 'BAD WHEEL', 'cluster': 'Brake & Tire Issues'},
        {'comment': 'DIDNT WORK OUT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'WON\'T HOLD CHARGE LONG ENOUGH', 'cluster': 'Battery & Charging Issues'},
        {'comment': 'DONT LIKE IT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'DINDT LIKE THE BIKE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'no need', 'cluster': 'General Dissatisfaction'},
        {'comment': 'not enough power', 'cluster': 'Performance Complaints'},
        {'comment': 'DIDNT NEED/ HOLE ON THE SDE', 'cluster': 'General Dissatisfaction'},
        {'comment': 'CAN\'T GET IT TO WORK RIGHT', 'cluster': 'General Dissatisfaction'},
        {'comment': 'doesnt hold up to its reputation', 'cluster': 'General Dissatisfaction'},
        {'comment': 'KICKSTAND/ERROR MESSAGE. DISPLAY', 'cluster': 'Electrical / Assembly Problems'}
    ]

#     # Create DataFrame
    df = pd.DataFrame(data)
    print(len(df))

    # Basic cleaning (optional)
    df['comment'] = df['comment'].astype(str).str.strip()
    df['cluster'] = df['cluster'].astype('category')

    # Aggregate counts
    counts = df['cluster'].value_counts().reset_index()
    counts.columns = ['cluster', 'count']

    # Plot settings
    sns.set(style="whitegrid")
    palette = sns.color_palette("tab10", n_colors=len(counts))

    # Horizontal bar chart (ranked)
    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=counts.sort_values('count', ascending=True),
        x='count', y='cluster',
        palette=palette
    )
    plt.title("Return Comments by Cluster")
    plt.xlabel("Number of Comments")
    plt.ylabel("")
    for i, (count, cluster) in enumerate(zip(counts.sort_values('count', ascending=True)['count'],
                                             counts.sort_values('count', ascending=True)['cluster'])):
        plt.text(count + 0.5, i, str(count), va='center')
    plt.tight_layout()
    plt.show()

    # Pie chart (percentage)
    plt.figure(figsize=(7, 7))
    plt.pie(
        counts['count'],
        labels=counts['cluster'],
        autopct='%1.1f%%',
        startangle=140,
        colors=palette
    )
    plt.title("Return Comments Distribution")
    plt.tight_layout()
    plt.show()

    # Optional: Save DF for later use
    file_path = fr"C:\Users\joshu\Documents\Shopify_API\return_comments_clustered.xlsx"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_excel(file_path, index=False)
    print(f"Excel file written to: {file_path}")



# clustered_comments()