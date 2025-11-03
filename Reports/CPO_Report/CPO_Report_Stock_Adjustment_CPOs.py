import os
import pandas as pd

from Unleashed_Data.Unleashed_Clean_Parallel import *
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel

def Unleashed_PowerBI_Costco_Returns(start_date, end_date):
    df_stock_adjustment = Unleashed_stock_adjustment_clean_data_parallel(start_date=start_date, reload=True,
                                                                         save_excel=True)


    # cosmo returns stock adjustment
    CPO_codes = ['CPO23150052', 'CPO23150051']
    df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment.loc[
        (df_stock_adjustment['ProductCode'].isin(CPO_codes))
        & (df_stock_adjustment['Status'] == 'Completed')].copy()
    print("THING: ", df_stock_adjustment_costco_returns_CPOcosmos_completed.columns)

    df_stock_adjustment_costco_returns_CPOcosmos_completed['Return Quantity'] = -1
    df_stock_adjustment_costco_returns_CPOcosmos_completed['Location'] = df_stock_adjustment_costco_returns_CPOcosmos_completed['WarehouseCode']
    df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment_costco_returns_CPOcosmos_completed.loc[df_stock_adjustment_costco_returns_CPOcosmos_completed['Location'] == 'Costco Returns']
    df_stock_adjustment_costco_returns_CPOcosmos_completed = df_stock_adjustment_costco_returns_CPOcosmos_completed[
        ['AdjustmentNumber', 'AdjustmentDate', 'ProductCode', 'ProductDescription', 'Return Quantity', 'Location']]
    df_stock_adjustment_costco_returns_CPOcosmos_completed.rename(columns={'AdjustmentDate': 'Date'}, inplace=True)
    df_stock_adjustment_costco_returns_CPOcosmos_completed['Bike Type'] = 'Cosmo 2.0 T CPO'
    df_stock_adjustment_costco_returns_CPOcosmos_completed['Customer Type'] = 'Costco Stock Adjustment'



    return df_stock_adjustment_costco_returns_CPOcosmos_completed


def Unleashed_PowerBI_Costco_Returns2(start_date, end_date, reload, save_excel=False):
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

