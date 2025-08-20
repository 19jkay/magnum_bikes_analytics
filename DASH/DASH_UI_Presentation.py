from rapidfuzz import process

from Product_Forecasting.Product_Forecast_Clean import *
# from DASH.DASH_main import dash_bike_launch, dash_bike_reload, dash_cosmo_black_bike_launch, dash_cosmo_calypso_bike_launch, cosmo_dash
from DASH.DASH_main import dash_bike_launch, dash_parts_launch, dash_parts_other_launch, dash_accessories_launch, dash_accessories_other_launch, dash_reload, dash_cosmo_black_bike_launch, dash_cosmo_calypso_bike_launch, cosmo_dash

from Product_Forecasting.Product_Forecasting_Helpers import get_date_info


def find_best_matches(user_input, choices, limit=10, threshold=60):
    matches = process.extract(user_input, choices, limit=limit, score_cutoff=threshold)
    return [match[0] for match in matches]



#get date info
today_str, last_day_prev_month_str = get_date_info()
print("Today:", today_str)
print("Last day of previous month:", last_day_prev_month_str)

#load data
reload_data = False
save_excel = True
df_bikes_list, df_accessories_list, df_parts = Unleashed_product_forecast_data(start_date='2022-01-01', end_date=last_day_prev_month_str, reload=reload_data, save_excel=save_excel)

#organize data
df_accessories_top_5, df_accessories_other = df_accessories_list[0], df_accessories_list[1]
df_bikes, df_bikes_descriptions = df_bikes_list[0], df_bikes_list[1]

#ensure data is truncated at last day of last month
df_bikes = df_bikes.loc[df_bikes['Year-Month'] <= last_day_prev_month_str]
df_bikes_descriptions = df_bikes_descriptions.loc[df_bikes_descriptions['Year-Month'] <= last_day_prev_month_str]
df_accessories_top_5 = df_accessories_top_5.loc[df_accessories_top_5['Year-Month'] <= last_day_prev_month_str]
df_accessories_other = df_accessories_other.loc[df_accessories_other['Year-Month'] <= last_day_prev_month_str]
df_parts = df_parts.loc[df_parts['Year-Month'] <= last_day_prev_month_str]

special_bikes = ['Cosmo 2.0 T - Black- 48v 15 Ah', 'Cosmo 2.0 T - Calypso - 48v 15 Ah']




# print(df_bikes_descriptions)
# print(df_bikes_descriptions['ProductDescription'].unique().tolist())

presentation = input("Presentation? (y/n): ")

if presentation == 'n':

    bike_names = df_bikes_descriptions['ProductDescription'].unique().tolist()

    user_input = input("Enter bike name or partial name: ")
    matches = find_best_matches(user_input, bike_names)
    print(matches)

    if matches:
        print("\nTop matches:")
        for i, name in enumerate(matches, 1):
            print(f"{i}. {name}")
    else:
        print("No close matches found.")

    product_name = matches[int(input("Enter number for final product name: ")) - 1]

    reload_dash_app = input("Start brand new dash app? (y/n): ")



    if reload_dash_app == "y":
        #have all of these be user input
        # product_name = 'Ranger 2.0 - BLK/WHT - 48V 20Ah'
        value_string = 'OrderQuantity'
        retrain = False
        path = 'Bike_Descriptions'
        poll_forecast = [1, 2, 3, 4, 5, 6]
        forecast_horizon = 6

        #Cosmo black
        if product_name ==  'Cosmo 2.0 T - Black- 48v 15 Ah':
            print("hit Costco")
            cosmo_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah'].reset_index(drop=True)
            lowrider_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == 'Low rider 2.0 - Black-Copper - 48v 15Ah'].reset_index(drop=True)
            dash_cosmo_black_bike_launch(cosmo_black_bike=cosmo_black_bike, lowrider_black_bike=lowrider_black_bike, product_name=product_name, value_string=value_string, path=path, forecast_horizon=forecast_horizon)

        #Cosmo calypso
        elif product_name == 'Cosmo 2.0 T - Calypso - 48v 15 Ah':
            print("Calypso hit")
            scaler = 0.15
            cosmo_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah'].reset_index(drop=True)
            lowrider_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == 'Low rider 2.0 - Black-Copper - 48v 15Ah'].reset_index(drop=True)
            #adjust data
            cosmo_black_bike['OrderQuantity'] = scaler * cosmo_black_bike['OrderQuantity']
            lowrider_black_bike['OrderQuantity'] = scaler * lowrider_black_bike['OrderQuantity']

            dash_cosmo_calypso_bike_launch(cosmo_black_bike=cosmo_black_bike, lowrider_black_bike=lowrider_black_bike, product_name=product_name, value_string=value_string, path=path, forecast_horizon=forecast_horizon)

        #Other bikes
        else:
            product_series = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == product_name].reset_index(drop=True)
            dash_bike_launch(series=product_series, financial_forecast=poll_forecast, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)

    else:
        path_segment = 'Product_Forecast'
        filename_suffix = "consensus_data"
        extension = "xlsx"

        # Build dynamic output path
        output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path_segment)

        # Clean filename components
        safe_product_name = product_name.replace("/", "_").replace(":", "_")
        filename = f"{safe_product_name}_{filename_suffix}.{extension}"

        # Full save path
        full_path = os.path.join(output_dir, filename)

        df = pd.read_excel(full_path)

        dash_reload(df, product_name)

else:
    product = input("Would you like to view Cosmo black or calypso? (b/c): ")

    if product == "b":
        product_name = 'Cosmo 2.0 T - Black- 48v 15 Ah Pres'
        reload = input("Start brand new dash app? (y/n): ")
        if reload == "y":
            cosmo_black_forecast_series = [882, 1300, 400, 1400, 1000, 402]
            poll_forecast = [882, 1300, 1400, 1400, 1000, 400]
            final_consensus = [882, 1300, 900, 1400, 1000, 401]
            inventory = [1196, 0, 0, 0, 0, 0]
            purchases = [790, 500, 2104, 1000, 500, 0]
            dates = ['2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01', '2026-01-01']
            cosmo_dash(cosmo_black_forecast_series, poll_forecast, final_consensus, inventory, purchases, dates, product_name)
        else:
            path_segment = 'Product_Forecast'
            filename_suffix = "consensus_data"
            extension = "xlsx"

            # Build dynamic output path
            output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path_segment)

            # Clean filename components
            safe_product_name = product_name.replace("/", "_").replace(":", "_")
            filename = f"{safe_product_name}_{filename_suffix}.{extension}"

            # Full save path
            full_path = os.path.join(output_dir, filename)

            df = pd.read_excel(full_path)

            dash_reload(df, product_name)

    #start calypso
    else:
        product_name = 'Cosmo 2.0 T - Calypso - 48v 15 Ah Pres'
        reload = input("Start brand new dash app? (y/n): ")
        if reload == "y":
            cosmo_black_forecast_series = [80, 80, 100, 350, 200, 200]
            poll_forecast = [80, 80, 200, 350, 200, 200]
            final_consensus = [80, 80, 150, 350, 200, 200]
            inventory = [20, 0, 0, 0, 0, 0]
            purchases = [150, 0, 500, 300, 200, 0]
            dates = ['2025-08-01', '2025-09-01', '2025-10-01', '2025-11-01', '2025-12-01', '2026-01-01']
            cosmo_dash(cosmo_black_forecast_series, poll_forecast, final_consensus, inventory, purchases, dates, product_name)

        else:
            path_segment = 'Product_Forecast'
            filename_suffix = "consensus_data"
            extension = "xlsx"

            # Build dynamic output path
            output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path_segment)

            # Clean filename components
            safe_product_name = product_name.replace("/", "_").replace(":", "_")
            filename = f"{safe_product_name}_{filename_suffix}.{extension}"

            # Full save path
            full_path = os.path.join(output_dir, filename)

            df = pd.read_excel(full_path)

            dash_reload(df, product_name)



