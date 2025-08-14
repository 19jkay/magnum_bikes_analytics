import pandas as pd
import os
import sys


from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_SalesOrders_clean_data_parallel



pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


def Unleashed_product_forecast_data(start_date, end_date, reload=True, save_excel=False):

    if reload:
        df = Unleashed_SalesOrders_clean_data_parallel(start_date=start_date, end_date=end_date, reload=reload, save_excel=save_excel)

    else:
        CLEAN_SALESORDERS_FILENAME = r"C:\Users\joshu\Documents\Unleashed_API\unleashed_parallel_clean_SalesOrders_data.xlsx"
        df = pd.read_excel(CLEAN_SALESORDERS_FILENAME)


    # df = df.loc[~df['Completed Date'].isna()] #remove nan dates
    # df_invoices['SubTotal'] = df_invoices['SubTotal'].str.replace(',', '').astype(float) #convert 'Sub Total' column to float datatype
    # df_invoices['OrderQuantity'] = df_invoices['OrderQuantity'].str.replace(',', '').astype(float)
    df['CompletedDate'] = pd.to_datetime(df['CompletedDate'])
    df['Year-Month'] = df['CompletedDate'].dt.to_period('M')
    df['Year-Month'] = df['Year-Month'].dt.to_timestamp()
    df = df.groupby(['Year-Month', 'ProductGroup', 'ProductDescription'])[['OrderQuantity', 'LineTotal']].sum().reset_index()





    #used for filling in full dates for accessories and bikes
    full_dates = pd.date_range(start=df['Year-Month'].min(),
                               end=df['Year-Month'].max(),
                               freq='MS')  # 'MS' = Month Start
    full_df = pd.DataFrame({'Year-Month': full_dates})
    full_df['LineTotal'] = 0
    full_df['OrderQuantity'] = 0


    #start dividing dataframes into groups
    product_groups = ['Accessories', 'Accessories SLC Store', 'Apparel', 'Battery',
                      'Bikes', 'Bikes EOL', 'Bottom Brackets', 'Brakes', 'Chargers',
                      'Cockpit', 'Controllers', 'Conversion Kit', 'Derailleur Hangers',
                      'Displays', 'Drivetrain', 'Electronics', 'Fenders', 'Forks', 'Frame',
                      'Headset', 'Lights', 'Marketing', 'Miscellaneous', 'Motor Wheels', 'Motors',
                      'Obsolete', 'PAS', 'Racks', 'Scooters', 'Shifters', 'Throttles', 'Tires', 'Tubes',
                      'Wheels', 'Derailleurs', 'International', 'Bundles', 'Warranty', 'Ecomm', 'Discount', 'Pre-sale']


    #df accessories
    df_accessories = df.loc[(df['ProductGroup'] == 'Accessories') | (df['ProductGroup'] == 'Accessories SLC Store')]
    # df_accessories = df_accessories.groupby(['Year-Month'])[['Product', 'Quantity', 'Sub Total']].sum().reset_index()
    df_accessories = df_accessories.groupby(['Year-Month', 'ProductDescription'])[['OrderQuantity', 'LineTotal']].sum().reset_index()

    #see top accessories
    # print((df_accessories.groupby('Product')['Sub Total'].sum().reset_index()).sort_values(by='Sub Total', ascending=False))

    top_five_accessories = ['Lock - Magnum Foldylock', 'Pannier Bag - Magnum Pannier w/anti-theft lock',
                            'Pannier Bag - Left - Payload',
                            'Rearview Mirror - Universal', 'Magnum Bike Cover']

    df_accessories_top_5 = df_accessories.loc[df_accessories['ProductDescription'].isin(top_five_accessories)]

    #now fill in missing dates for the accessories
    accessories_top_5_df_list = []
    accessory_top_5_types = df_accessories_top_5['ProductDescription'].unique()

    for accessory_type in accessory_top_5_types:
        one_type = df_accessories_top_5.loc[df_accessories_top_5['ProductDescription'] == accessory_type].reset_index(drop=True)

        filled_df = pd.merge(full_df, one_type, on='Year-Month', how='left', suffixes=('_full', '_orig'))

        filled_df['LineTotal'] = filled_df['LineTotal_orig'].fillna(filled_df['LineTotal_full'])
        filled_df['OrderQuantity'] = filled_df['OrderQuantity_orig'].fillna(filled_df['OrderQuantity_full'])
        filled_df['ProductDescription'] = filled_df['ProductDescription'].fillna(accessory_type)

        new_one_accessory_type = filled_df[['Year-Month', 'ProductDescription', 'OrderQuantity', 'LineTotal']]
        accessories_top_5_df_list.append(new_one_accessory_type)
        # employee_df['Sub Total'] = employee_df['Sub Total'].clip(lower=0)

    df_accessories_top_5 = pd.concat(accessories_top_5_df_list, ignore_index=True)

    #accessories that are not top 5
    df_accessories_other = df_accessories.loc[~df_accessories['ProductDescription'].isin(top_five_accessories)]
    df_accessories_other = df_accessories_other.groupby(['Year-Month'])[['OrderQuantity', 'LineTotal']].sum().reset_index()

    df_accessories_list = [df_accessories_top_5, df_accessories_other]


    #begin parts
    parts_groups = ['Battery',
                    'Bottom Brackets', 'Brakes', 'Chargers',
                    'Cockpit', 'Controllers', 'Conversion Kit', 'Derailleur Hangers',
                    'Displays', 'Drivetrain', 'Electronics', 'Fenders', 'Forks', 'Frame',
                    'Headset', 'Lights', 'Motor Wheels', 'Motors',
                    'Racks', 'Scooters', 'Shifters', 'Throttles', 'Tires', 'Tubes',
                    'Wheels', 'Derailleurs']

    df_parts = df.loc[df['ProductGroup'].isin(parts_groups)]
    df_parts = df_parts.groupby(['Year-Month'])[['OrderQuantity', 'LineTotal']].sum().reset_index()

    df_bikes = df.loc[df['ProductGroup'] == 'Bikes'].copy()
    df_bikes['Bike_type'] = df_bikes['ProductDescription'].str.extract(r'^\s*(.*?)\s*-\s*')
    df_bikes = df_bikes.groupby(['Year-Month', 'Bike_type'])[['OrderQuantity', 'LineTotal']].sum().reset_index()


    #fill in missing data
    bike_types = df_bikes['Bike_type'].unique()

    bikes_df_list = []

    for bike_type in bike_types:
        one_bike_type = df_bikes.loc[df_bikes['Bike_type'] == bike_type].reset_index(drop=True)

        filled_df = pd.merge(full_df, one_bike_type, on='Year-Month', how='left', suffixes=('_full', '_orig'))

        filled_df['LineTotal'] = filled_df['LineTotal_orig'].fillna(filled_df['LineTotal_full'])
        filled_df['OrderQuantity'] = filled_df['OrderQuantity_orig'].fillna(filled_df['OrderQuantity_full'])
        filled_df['Bike_type'] = filled_df['Bike_type'].fillna(bike_type)

        new_one_bike_type = filled_df[['Year-Month', 'Bike_type', 'OrderQuantity', 'LineTotal']]
        bikes_df_list.append(new_one_bike_type)
        # employee_df['Sub Total'] = employee_df['Sub Total'].clip(lower=0)

    df_bikes = pd.concat(bikes_df_list, ignore_index=True)

    # pd.set_option('display.max_rows', None)
    # print(df_bikes)




    #df bikes with descriptions to forecast on description level
    df_bikes_descriptions = df.loc[df['ProductGroup'] == 'Bikes']
    df_bikes_descriptions = df_bikes_descriptions.groupby(['Year-Month', 'ProductDescription'])[['OrderQuantity', 'LineTotal']].sum().reset_index()

    #now fill in missing dates with 0 for each bike description
    bike_description_types = df_bikes_descriptions['ProductDescription'].unique()

    bikes_descriptions_df_list = []
    for bike_description_type in bike_description_types:
        one_bike_type = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == bike_description_type].reset_index(drop=True)

        filled_df = pd.merge(full_df, one_bike_type, on='Year-Month', how='left', suffixes=('_full', '_orig'))

        filled_df['LineTotal'] = filled_df['LineTotal_orig'].fillna(filled_df['LineTotal_full'])
        filled_df['OrderQuantity'] = filled_df['OrderQuantity_orig'].fillna(filled_df['OrderQuantity_full'])
        filled_df['ProductDescription'] = filled_df['ProductDescription'].fillna(bike_description_type)

        new_one_bike_type = filled_df[['Year-Month', 'ProductDescription', 'OrderQuantity', 'LineTotal']]
        bikes_descriptions_df_list.append(new_one_bike_type)
        # employee_df['Sub Total'] = employee_df['Sub Total'].clip(lower=0)

    df_bikes_descriptions = pd.concat(bikes_descriptions_df_list, ignore_index=True)


    df_bikes_list = [df_bikes, df_bikes_descriptions]

    return df_bikes_list, df_accessories_list, df_parts

#
# reload_data = False
# a, b, c = Unleashed_product_forecast_data(start_date='2022-01-01', end_date='2025-06-30', reload=reload_data)
#
# print(a)
# print(b)
# print(c)