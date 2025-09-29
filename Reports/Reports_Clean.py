from Unleashed_Data.Unleashed_Clean_Parallel import *
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


def Unleashed_PowerBI_Costco_Returns(reload):
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

    # Paste the list produced earlier (or load from file). Example variable name: clustered_comments
    clustered_comments = [
        {"comment": "NOT CHARGINGC/D", "cluster": "Battery & Charging Issues"},
        {"comment": "CORD IS NOT LONG ENOUGH", "cluster": "Electrical / Assembly Problems"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "TIRES NOT FUNCTIONING", "cluster": "Brake & Tire Issues"},
        {"comment": "QUIT WORKING", "cluster": "General Dissatisfaction"},
        {"comment": "ROTTER ON FRONT TIRE BENT", "cluster": "Brake & Tire Issues"},
        {"comment": "USED", "cluster": "General Dissatisfaction"},
        {"comment": "TOO FAST", "cluster": "Performance Complaints"},
        {"comment": "D RAIL IS SLIPPING", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "IT WOULD NOT CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "BRAKES DONT WORK WELL", "cluster": "Brake & Tire Issues"},
        {"comment": "GOTB THE WRONG ONE", "cluster": "General Dissatisfaction"},
        {"comment": "PEDAL BROKE", "cluster": "Mechanical Failures"},
        {"comment": "DERAILER IS OFF LIGHT ISNT BRIGHT", "cluster": "Electrical / Assembly Problems"},
        {"comment": "DOESN'T WANT", "cluster": "General Dissatisfaction"},
        {"comment": "USED. TOO HEAVY AND TOO BIG", "cluster": "Too Heavy / Too Big"},
        {"comment": "BREAKS ARE SQUELING", "cluster": "Brake & Tire Issues"},
        {"comment": "DOESNT WORK", "cluster": "General Dissatisfaction"},
        {"comment": "DW", "cluster": "General Dissatisfaction"},
        {"comment": "BATTERY WILL NOTCHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "DEFECTIVE", "cluster": "General Dissatisfaction"},
        {"comment": "SEAT KEPT MOVING", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "WIFE SAID NO", "cluster": "General Dissatisfaction"},
        {"comment": "TOO HEAVY NOTHING WRONG WITH IT", "cluster": "Too Heavy / Too Big"},
        {"comment": "USED– TOO FAST FOR NEEDS", "cluster": "Performance Complaints"},
        {"comment": "UDNW/ BATTERY DOES NOT WRK WELL", "cluster": "Battery & Charging Issues"},
        {"comment": "REAR TIRE BLEW OFF THE BEAD. IT IS FLAT.", "cluster": "Brake & Tire Issues"},
        {"comment": "BOUGHT TODAY CORD CUT", "cluster": "Electrical / Assembly Problems"},
        {"comment": "WIRE FOR LIGHT TO SHORT", "cluster": "Electrical / Assembly Problems"},
        {"comment": "BATTERY DOES NOT CHARGE AT ALL", "cluster": "Battery & Charging Issues"},
        {"comment": "FOOT EDAL FELL OFF", "cluster": "Mechanical Failures"},
        {"comment": "PUT TOGETHER WRONG", "cluster": "Electrical / Assembly Problems"},
        {"comment": "FLATTIRE", "cluster": "Brake & Tire Issues"},
        {"comment": "TO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "PART OF IT IS NOTWORKING", "cluster": "General Dissatisfaction"},
        {"comment": "SON DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "BATTER BAD AND SHIFTS GEARS ITSELF ONHIL", "cluster": "Battery & Charging Issues"},
        {"comment": "WIRE ASSEMBLE UNABLE TO ATTACH BASKET", "cluster": "Electrical / Assembly Problems"},
        {"comment": "WIFE DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT WORK", "cluster": "General Dissatisfaction"},
        {"comment": "ONE BOLT IS STRIPPED", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "NO WANT", "cluster": "General Dissatisfaction"},
        {"comment": "JUST WANTED TO TRY OUT", "cluster": "General Dissatisfaction"},
        {"comment": "DEJO DE CARGA", "cluster": "Battery & Charging Issues"},
        {"comment": "BRAKES DO NOT WORK", "cluster": "Brake & Tire Issues"},
        {"comment": "DIDNT NEED IT NOTHING WRONG", "cluster": "General Dissatisfaction"},
        {"comment": "DIES QUICK", "cluster": "Battery & Charging Issues"},
        {"comment": "BENT FRAME", "cluster": "Mechanical Failures"},
        {"comment": "WIRE PREVENTS STEERING", "cluster": "Electrical / Assembly Problems"},
        {"comment": "DID NOT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT NEED", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "DOESNT CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "DOESNT WORK", "cluster": "General Dissatisfaction"},
        {"comment": "BACK TIRE HAS LEAK", "cluster": "Brake & Tire Issues"},
        {"comment": "BAD MAKES ALOT OF NOISE", "cluster": "Performance Complaints"},
        {"comment": "AIR LEAKS IN TIRES", "cluster": "Brake & Tire Issues"},
        {"comment": "WAS HARD TO ASSEMBLE", "cluster": "Electrical / Assembly Problems"},
        {"comment": "BREAKS DONT WORK", "cluster": "Brake & Tire Issues"},
        {"comment": "PEOPLE GOT HURT ON IT", "cluster": "Performance Complaints"},
        {"comment": "WAYYY TOO HEAVY AND BIG", "cluster": "Too Heavy / Too Big"},
        {"comment": "NOT HOLDING A CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "PEDALS FELL OFF", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "NOT WORKING", "cluster": "General Dissatisfaction"},
        {"comment": "BATTERY WONT HOLD CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "GEARS SLIPPINGCHANGED MIND", "cluster": "Mechanical Failures"},
        {"comment": "DOESNT THINK THEYLL USE IT", "cluster": "General Dissatisfaction"},
        {"comment": "SLIGHTLY USED TOO BIG FOR PERSON", "cluster": "Too Heavy / Too Big"},
        {"comment": "DOESNT CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "WASNT WORKING FOR THEM", "cluster": "General Dissatisfaction"},
        {"comment": "STRIPPED BIKE PEDAL AREA", "cluster": "Mechanical Failures"},
        {"comment": "DOESNT WORK", "cluster": "General Dissatisfaction"},
        {"comment": "OCW USED NOT SATISFIED", "cluster": "General Dissatisfaction"},
        {"comment": "TO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "OPENED", "cluster": "General Dissatisfaction"},
        {"comment": "ECOM/TOO BIG", "cluster": "Too Heavy / Too Big"},
        {"comment": "FULLY CHARGED BUT ELECTRICS WONT WORK", "cluster": "Electrical / Assembly Problems"},
        {"comment": "BATTERY NOT CHARGIN", "cluster": "Battery & Charging Issues"},
        {"comment": "WONT HOLD A CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "UNCONFORTABLE", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT WANT", "cluster": "General Dissatisfaction"},
        {"comment": "NOT POWERFUL ENOUGH", "cluster": "Performance Complaints"},
        {"comment": "DEFECTIVE", "cluster": "General Dissatisfaction"},
        {"comment": "TRIED DONT NEED", "cluster": "General Dissatisfaction"},
        {"comment": "WRONG ONE", "cluster": "General Dissatisfaction"},
        {"comment": "COULDNT GET TO WORK OTHER ONE DID", "cluster": "General Dissatisfaction"},
        {"comment": "CHAIN COMES OFF– VERY HEAVY TOO!!!!", "cluster": "Mechanical Failures"},
        {"comment": "PEDAL CAME OFF", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "DNN", "cluster": "General Dissatisfaction"},
        {"comment": "CABLE IS WRAPPED WRONG WAY", "cluster": "Electrical / Assembly Problems"},
        {"comment": "DID NOT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "REALLY HEAVY STARTS TO CLICK WHEN GOING", "cluster": "Mechanical Failures"},
        {"comment": "DID NOT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "TO BIG", "cluster": "Too Heavy / Too Big"},
        {"comment": "TOO BIG", "cluster": "Too Heavy / Too Big"},
        {"comment": "DIDNT NEED", "cluster": "General Dissatisfaction"},
        {"comment": "BACK WHEEL BENT", "cluster": "Brake & Tire Issues"},
        {"comment": "PEDAL ASSIST DOES NOT WORK", "cluster": "Battery & Charging Issues"},
        {"comment": "DIDNT NEED", "cluster": "General Dissatisfaction"},
        {"comment": "TONS OF ISSUES", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "FOUND BETTER DEAL", "cluster": "General Dissatisfaction"},
        {"comment": "BIKE LAUNCHES FORWARD UNEXPECTANTLY", "cluster": "Performance Complaints"},
        {"comment": "NOT FUNCTIONING PROPERLY", "cluster": "General Dissatisfaction"},
        {"comment": "NEVER CHARGED NEVER WORKED OUT OF BOX", "cluster": "Battery & Charging Issues"},
        {"comment": "USED", "cluster": "General Dissatisfaction"},
        {"comment": "BATTERY DONT HOLD CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "DIDN'T NEED – OPENED", "cluster": "General Dissatisfaction"},
        {"comment": "WANTS ANOTHER ONE", "cluster": "General Dissatisfaction"},
        {"comment": "MISSING SCREWS", "cluster": "Electrical / Assembly Problems"},
        {"comment": "TOO BIG", "cluster": "Too Heavy / Too Big"},
        {"comment": "CHAIN CAME OFF", "cluster": "Mechanical Failures"},
        {"comment": "USED NOT HOLD CHARGE VERY LONG", "cluster": "Battery & Charging Issues"},
        {"comment": "OPEN CAME DAMAGE", "cluster": "Mechanical Failures"},
        {"comment": "MAMKING WERID STOPS", "cluster": "Performance Complaints"},
        {"comment": "THEY BOUGHT A RETURN BUT IT DID NOT WORK", "cluster": "General Dissatisfaction"},
        {"comment": "DW", "cluster": "General Dissatisfaction"},
        {"comment": "TO BIG OPENED TRYED", "cluster": "Too Heavy / Too Big"},
        {"comment": "TO SHORT FOR IT", "cluster": "Too Heavy / Too Big"},
        {"comment": "BATTERY GETS TOO HOT", "cluster": "Battery & Charging Issues"},
        {"comment": "FALLING APART", "cluster": "Mechanical Failures"},
        {"comment": "MAKE NOISE", "cluster": "Performance Complaints"},
        {"comment": "HAD A FLAT TIRE", "cluster": "Brake & Tire Issues"},
        {"comment": "WONT CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "FLAT TIRE", "cluster": "Brake & Tire Issues"},
        {"comment": "NOT HOLDING CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "TO BIG FOR CHILD", "cluster": "Too Heavy / Too Big"},
        {"comment": "DIDNT T LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "BATTERY DIDN'T WORK IT DOESN'T TURN ON", "cluster": "Battery & Charging Issues"},
        {"comment": "BAD", "cluster": "General Dissatisfaction"},
        {"comment": "HAS AS SHORT IN THE WIRING", "cluster": "Electrical / Assembly Problems"},
        {"comment": "FRONT PIECE BROKEN", "cluster": "Mechanical Failures"},
        {"comment": "RECIEVED BOX DAMAGED/ NOT OPENED", "cluster": "Mechanical Failures"},
        {"comment": "CABLE IS NOTLONG ENOUGH ON LIGHT", "cluster": "Electrical / Assembly Problems"},
        {"comment": "WONT CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "NO LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "TOO MUCH GOING OUTN", "cluster": "Performance Complaints"},
        {"comment": "JUST NOT WORTH THE MONEY", "cluster": "General Dissatisfaction"},
        {"comment": "847 WONT TURN ON", "cluster": "Battery & Charging Issues"},
        {"comment": "SQEAKING", "cluster": "Performance Complaints"},
        {"comment": "TOO TALL", "cluster": "Too Heavy / Too Big"},
        {"comment": "CHAIN BROKE", "cluster": "Mechanical Failures"},
        {"comment": "DIDNT LIE", "cluster": "General Dissatisfaction"},
        {"comment": "DINDT WANT", "cluster": "General Dissatisfaction"},
        {"comment": "NOT VERY FAST DONT LIKE IT", "cluster": "Performance Complaints"},
        {"comment": "BATTERY LIFE", "cluster": "Battery & Charging Issues"},
        {"comment": "IT A CRAPPY BIKE", "cluster": "General Dissatisfaction"},
        {"comment": "KEEPS SHUTTING OFF RANDOMLY", "cluster": "Battery & Charging Issues"},
        {"comment": "USED DOESNT PEDAL COMES OFF", "cluster": "Mechanical Failures"},
        {"comment": "HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "TOO BIG FOR KIDS", "cluster": "Too Heavy / Too Big"},
        {"comment": "WRONG ITEM ADVERTISED", "cluster": "General Dissatisfaction"},
        {"comment": "BAD BATTERY WONT HOLD CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "WASNT CHARGING / BOUGHT LESS THAN A WEEK", "cluster": "Battery & Charging Issues"},
        {"comment": "TOO HEAVY TO USE", "cluster": "Too Heavy / Too Big"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "BROKEN", "cluster": "Mechanical Failures"},
        {"comment": "NO LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "DOES NOT WORK PROPERLY", "cluster": "General Dissatisfaction"},
        {"comment": "TIRE IS FLAT", "cluster": "Brake & Tire Issues"},
        {"comment": "SEAT DOESNT STAYUP", "cluster": "Mechanical Failures"},
        {"comment": "NEVER OPENED DAMAGED BOX", "cluster": "Mechanical Failures"},
        {"comment": "TOO BIG TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "ELECTRIC PART NOT WORKING", "cluster": "Battery & Charging Issues"},
        {"comment": "CANT GO UPHILL AS FAST AS DOWNHILL", "cluster": "Performance Complaints"},
        {"comment": "DIDNT WANT NEED", "cluster": "General Dissatisfaction"},
        {"comment": "NOT CHARGING", "cluster": "Battery & Charging Issues"},
        {"comment": "DIDNT LIKE/ USED", "cluster": "General Dissatisfaction"},
        {"comment": "GOT WRONG COLOR", "cluster": "General Dissatisfaction"},
        {"comment": "CABLE BROKE ON FRONT", "cluster": "Mechanical Failures"},
        {"comment": "BREAKS ACTING WEIRD", "cluster": "Brake & Tire Issues"},
        {"comment": "DOESNT WORK", "cluster": "Battery & Charging Issues"},
        {"comment": "USED –TOO POWERFUL", "cluster": "Performance Complaints"},
        {"comment": "BATTERY ISSUE", "cluster": "Battery & Charging Issues"},
        {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "ECOM– WANTED A DIFERENT ONE", "cluster": "General Dissatisfaction"},
        {"comment": "NEVER PROPERLY WORKED.", "cluster": "Battery & Charging Issues"},
        {"comment": "TOO JERKY", "cluster": "Performance Complaints"},
        {"comment": "CLICKING AND GEARS SLIPPING", "cluster": "Mechanical Failures"},
        {"comment": "ECOM", "cluster": "General Dissatisfaction"},
        {"comment": "ECOM", "cluster": "General Dissatisfaction"},
        {"comment": "MECHANISM WON'T UNBOLT FOR LIGHT", "cluster": "Electrical / Assembly Problems"},
        {"comment": "ECOMM ARRIVED LATE", "cluster": "General Dissatisfaction"},
        {"comment": "THE BRAKE BROKE", "cluster": "Brake & Tire Issues"},
        {"comment": "SOMETIMES WORKS/ ERRRO", "cluster": "Battery & Charging Issues"},
        {"comment": "NOT FAST ENOUGH", "cluster": "Performance Complaints"},
        {"comment": "DIDNTWNAT", "cluster": "General Dissatisfaction"},
        {"comment": "ONLINE...NEW", "cluster": "General Dissatisfaction"},
        {"comment": "KEEPS STOPPING ON THEM", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "TOO HEAVYY FOR PERSON", "cluster": "Too Heavy / Too Big"},
        {"comment": "DOES NOTWORK FOR MEMBER", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT WANT", "cluster": "General Dissatisfaction"},
        {"comment": "NEVER WORKED", "cluster": "Battery & Charging Issues"},
        {"comment": "DIDNLTIKE", "cluster": "General Dissatisfaction"},
        {"comment": "STOPPED WORKING", "cluster": "Battery & Charging Issues"},
        {"comment": "MAKING NOISES", "cluster": "Performance Complaints"},
        {"comment": "MULTIPLE MALFUNCTIONS", "cluster": "Mechanical Failures"},
        {"comment": "DOESNT CHARGE OR START UP", "cluster": "Battery & Charging Issues"},
        {"comment": "NEITHER OF THEM CHARGE OR START UP", "cluster": "Battery & Charging Issues"},
        {"comment": "SQUEAKING BREAKS", "cluster": "Brake & Tire Issues"},
        {"comment": "BREAKS SQUEAKS AND BAD GEARS", "cluster": "Brake & Tire Issues"},
        {"comment": "NOT WORKING", "cluster": "General Dissatisfaction"},
        {"comment": "IT WON'T START EVEN FULL CHARGED", "cluster": "Battery & Charging Issues"},
        {"comment": "DOESNT CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "STOPPED WORKING", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT WANT – OK PER AMANDA", "cluster": "General Dissatisfaction"},
        {"comment": "USED", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "BROKEN", "cluster": "Mechanical Failures"},
        {"comment": "DNW", "cluster": "General Dissatisfaction"},
        {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "DIDNT LIKE BIKE", "cluster": "General Dissatisfaction"},
        {"comment": "SEAT BROKE WEAK LITTLE PIECES KEEPBREAK", "cluster": "Mechanical Failures"},
        {"comment": "USED", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "RETURN BC FELL OFF BIKE", "cluster": "Mechanical Failures"},
        {"comment": "FRONT TIRE FLAT F & B LIGHTS DON'T WORK", "cluster": "Brake & Tire Issues"},
        {"comment": "NOT GOOD FOR HOLDING SURFBOARD", "cluster": "General Dissatisfaction"},
        {"comment": "ELECTRONIC ISSUES", "cluster": "Battery & Charging Issues"},
        {"comment": "CHANGED MIND", "cluster": "General Dissatisfaction"},
        {"comment": "BOLT FELL OFF WHILE RIDING", "cluster": "Mechanical Failures"},
        {"comment": "CHAINS NOT WORKING", "cluster": "Mechanical Failures"},
        {"comment": "BROKEN", "cluster": "Mechanical Failures"},
        {"comment": "DAMAGED TIRE", "cluster": "Brake & Tire Issues"},
        {"comment": "UDW–WILL NOT TAKE ITS CHARGE", "cluster": "Battery & Charging Issues"},
        {"comment": "WIFE SAID NO", "cluster": "General Dissatisfaction"},
        {"comment": "DIDNT LIKE", "cluster": "General Dissatisfaction"},
        {"comment": "TIRE KEEPS BREAKING", "cluster": "Brake & Tire Issues"},
        {"comment": "DONT WANT RTND OTHER BIKE", "cluster": "General Dissatisfaction"},
        {"comment": "FRONT TIRE JERKS", "cluster": "Brake & Tire Issues"},
        {"comment": "WANTS A DIFFERENT BIKE", "cluster": "General Dissatisfaction"},
        {"comment": "TOO HEAVY FOR MEMBER", "cluster": "Too Heavy / Too Big"},
        {"comment": "TIRES WENT FLAT ON FIRST USE", "cluster": "Brake & Tire Issues"},
        {"comment": "DIDTN WANT", "cluster": "General Dissatisfaction"},
        {"comment": "DOESNT HAVE CHAIN GUARD", "cluster": "Mechanical Failures"},
        {"comment": "HAS A BREAK INWIRE", "cluster": "Electrical / Assembly Problems"},
        {"comment": "KICK STAND BROKE OFF", "cluster": "Mechanical Failures"},
        {"comment": "BACK RIM DAMAGED WHEN SHIPPED", "cluster": "Mechanical Failures"},
        {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "TOO HEAVY", "cluster": "Too Heavy / Too Big"},
        {"comment": "DOES NOT WORK", "cluster": "General Dissatisfaction"},
        {"comment": "NOT SHIFTING CORRECTLY", "cluster": "Mechanical Failures"}
    ]

    # Create DataFrame
    df = pd.DataFrame(clustered_comments)

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
    # df.to_csv("clustered_comments.csv", index=False)



# clustered_comments()