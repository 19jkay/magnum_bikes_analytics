from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel

import os
import pandas as pd


def AI_Automation_SalesOrders_clean(start_date, end_date, reload=True, save_excel=False):
    # This code basically gets a Sales Enquiry from Unleashed with Transaction Date = Completed Date and everything else default

    # rerun API Calls
    if reload:
        df_salesorders = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date, end_date=end_date)
        # df_products = get_data_parallel(unleashed_data_name="Products")
        # df_customers = get_data_parallel(unleashed_data_name="Customers")


        # df_salesorders = df_salesorders.loc[df_salesorders['OrderStatus'] == 'Completed']  # get completed invoices

        df_salesorders.rename(columns={'Product.ProductCode': 'ProductCode',
                                       'Product.ProductDescription': 'ProductDescription'}, inplace=True)

        # df_salesorders = df_salesorders[
        #     ['CustomerCode', 'CustomerName', 'CompletedDate', 'ProductCode', 'ProductDescription', 'OrderQuantity',
        #      'LineTotal']]

        df_salesorders['LineTotal'] = df_salesorders['LineTotal'].astype(float)  # convert 'Sub Total' column to float datatype
        # df_salesorders['OrderQuantity'] = df_salesorders['OrderQuantity'].astype(float)

        # The InvoiceLines column has a list of dictionaries for each individual purchase. Make each dictionary a row in the dataframe
        # Step 1: Create a copy without the 'InvoiceLines' column
        base_df = df_salesorders.drop(columns=['SerialNumbers'])
        # Step 2: Convert 'InvoiceLines' list of dicts into a separate DataFrame
        lines_df = df_salesorders[['SerialNumbers']].explode('SerialNumbers').reset_index()
        # Step 3: Normalize each dictionary into its own row
        salesorder_lines_expanded = pd.json_normalize(lines_df['SerialNumbers'])
        # Step 4: Merge base invoice info with each invoice line
        final_df = pd.concat([base_df.loc[lines_df['index']].reset_index(drop=True), salesorder_lines_expanded], axis=1)
        # final_df = final_df.rename(columns={'LastModifiedOn': 'SalesOrderLines_LastModifiedOn'})
        final_df = final_df.rename(columns={'LastModifiedOn': 'SarielNumber_LastModifiedOn'})
        final_df = final_df.rename(columns={'Guid': 'SerialNumber_Guid'})
        final_df = final_df.rename(columns={'Identifier': 'SerialNumber_Identifier'})


        final_df = final_df[['OrderNumber', 'CustomerCode', 'CustomerName', 'OrderDate', 'RequiredDate', 'CompletedDate', 'ProductCode', 'ProductDescription', 'SerialNumber_Identifier', 'SerialNumber_Guid', 'SarielNumber_LastModifiedOn']]
        final_df = final_df.rename(columns={'OrderNumber': 'SalesOrderNumber'})



        if save_excel:
            file_path = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_serialnumbers_data.xlsx"
            folder_path = os.path.dirname(file_path)
            os.makedirs(folder_path, exist_ok=True)
            final_df.to_excel(file_path, index=False)
            print(f"Excel file written to: {file_path}")

    else:
        SALESORDERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_serialnumbers_data.xlsx"
        final_df = pd.read_excel(SALESORDERS_FILENAME)

    return final_df