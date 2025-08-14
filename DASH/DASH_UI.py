from rapidfuzz import process

from Product_Forecasting.Product_Forecast_Clean import *
from DASH.DASH_main import dash_bike_launch, dash_bike_reload, dash_cosmo_black_bike_launch, dash_cosmo_calypso_bike_launch, cosmo_dash
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info


def find_best_matches(user_input, choices, limit=10, threshold=60):
    matches = process.extract(user_input, choices, limit=limit, score_cutoff=threshold)
    return [match[0] for match in matches]

def display_matches(user_input, choices):
    matches = find_best_matches(user_input, choices)

    if matches:
        print("\nTop matches:")
        for i, name in enumerate(matches, 1):
            print(f"{i}. {name}")
    else:
        print("No close matches found.")

    product_name = matches[int(input("Enter number for final product name: ")) - 1]
    return product_name

def reload_df_for_dash(product_name, path):
    # path_segment = r'Products\Product_SKUs'
    # df = reload_df_for_dash(product_name=product_name, path_segment=path_segment)
    filename_suffix = "consensus_data"
    extension = "xlsx"

    # Build dynamic output path
    output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path)

    # Clean filename components
    safe_product_name = product_name.replace("/", "_").replace(":", "_")
    filename = f"{safe_product_name}_{filename_suffix}.{extension}"

    # Full save path
    full_path = os.path.join(output_dir, filename)

    df = pd.read_excel(full_path)
    return df




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


forecasting_category = input("Enter forecasting category Product, Sales, Operations (p/s/o): ")

#go into product forecasting
if forecasting_category == 'p': #go into products
    print("Product Forecasting Category Chosen.")
    product_category = input("Enter product category Bikes, Parts, Accessories (b/p/a): ")
    if product_category == 'b': #go into bikes
        print("Product Category Bikes Chosen.")
        bike_category = input("Enter bike category Specific SKU or Bike Type (s/b): ")
        if bike_category == 's': #go into specific bike SKU

            user_input = input("Enter bike name or partial name: ")
            bike_names = df_bikes_descriptions['ProductDescription'].unique().tolist()
            product_name = display_matches(user_input=user_input, choices=bike_names)
            reload_dash_app = input("Start brand new dash app? (y/n): ")
            path = os.path.join("Product_Forecasting", "Bikes", "Bike_SKUs")


            if reload_dash_app == "y":
                value_string = 'OrderQuantity'
                retrain = False

                poll_forecast = [0, 0, 0, 0, 0, 0]
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
                    dash_bike_launch(series=product_series, financial_forecast=poll_forecast, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon, SKU_or_type='ProductDescription')

            else:
                df = reload_df_for_dash(product_name=product_name, path=path)
                dash_bike_reload(df, product_name, path)

        else: #go into specific bike type
            user_input = input("Enter bike type name or partial name: ")
            bike_names = df_bikes['Bike_type'].unique().tolist()
            product_name = display_matches(user_input=user_input, choices=bike_names)
            reload_dash_app = input("Start brand new dash app? (y/n): ")
            path = os.path.join("Product_Forecasting", "Bikes", "Bike_types")

            if reload_dash_app == "y": #go into dash app
                value_string = 'OrderQuantity'
                retrain = False

                poll_forecast = [0, 0, 0, 0, 0, 0]
                forecast_horizon = 6

                product_series = df_bikes.loc[df_bikes['Bike_type'] == product_name].reset_index(drop=True)
                dash_bike_launch(series=product_series, financial_forecast=poll_forecast, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon, SKU_or_type='Bike_type')

            else:
                df = reload_df_for_dash(product_name=product_name, path=path)
                dash_bike_reload(df, product_name, path)