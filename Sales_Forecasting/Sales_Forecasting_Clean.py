from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel
import pandas as pd
import os


def Unleashed_sales_forecast_data(start_date, end_date, reload=True, save_excel=False):

    if reload:
        df = Unleashed_SalesOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload, save_excel=save_excel)

        df = df[['CompletedDate', 'CustomerType', 'LineTotal']]
        df = df.loc[~df['CompletedDate'].isna()]  # remove nan dates
        df['LineTotal'] = df['LineTotal'].astype(float)  # convert 'Sub Total' column to float datatype
        df['CompletedDate'] = pd.to_datetime(df['CompletedDate'])
        df['Year-Month'] = df['CompletedDate'].dt.to_period('M')
        df['Year-Month'] = df['Year-Month'].dt.to_timestamp()

        df['CustomerType'] = df['CustomerType'].replace('', 'No Customer Type')
        df['CustomerType'] = df['CustomerType'].fillna('No Customer Type')

        df = df.groupby(['Year-Month', 'CustomerType'])['LineTotal'].sum().reset_index()

        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        CLEAN_SALESORDERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_data.xlsx"
        df = pd.read_excel(CLEAN_SALESORDERS_FILENAME)

    return df