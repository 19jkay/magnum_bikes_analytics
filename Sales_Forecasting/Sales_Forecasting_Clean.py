from Unleashed_Data.Unleashed_Clean import Unleashed_invoices_clean_data
from Unleashed_Data.Unleashed_Load import *



def Unleashed_sales_forecast_data(start_date, end_date, reload=True):

    if reload:
        df = Unleashed_invoices_clean_data(start_date=start_date, end_date=end_date, reload=reload)

    else:
        CLEAN_INVOICES_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_clean_Invoices_data.xlsx"
        df = pd.read_excel(CLEAN_INVOICES_FILENAME)


    df = df[['InvoiceDate', 'CustomerType', 'LineTotal']]
    df = df.loc[~df['InvoiceDate'].isna()] #remove nan dates
    df['LineTotal'] = df['LineTotal'].astype(float) #convert 'Sub Total' column to float datatype
    df['Completed Date'] = pd.to_datetime(df['InvoiceDate'])
    df['Year-Month'] = df['Completed Date'].dt.to_period('M')
    df['Year-Month'] = df['Year-Month'].dt.to_timestamp()
    df = df.groupby(['Year-Month', 'CustomerType'])['LineTotal'].sum().reset_index()

    return df