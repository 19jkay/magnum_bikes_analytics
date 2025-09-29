# from rapidfuzz import process
from rapidfuzz import fuzz, process
import time

from Product_Forecasting.Product_Forecast_Clean import *
from DASH.DASH_main import dash_bike_launch, dash_parts_launch, dash_parts_other_launch, dash_accessories_launch, \
    dash_accessories_other_launch, dash_reload, dash_cosmo_black_bike_launch, dash_cosmo_calypso_bike_launch, dash_ibd_bike_launch
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info
from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_Warehouses_clean_data_parallel
from Sales_Forecasting.Sales_Forecasting_Clean import Unleashed_sales_forecast_data

if __name__ == "__main__":
    def find_best_matches(user_input, choices, limit=10):
        def score(choice):
            # Normalize
            input_lower = user_input.lower()
            choice_lower = choice.lower()

            # Positional match score
            positional_score = sum(
                1 for i, c in enumerate(input_lower)
                if i < len(choice_lower) and choice_lower[i] == c
            )

            # Total shared letters score
            shared_letters = set(input_lower) & set(choice_lower)
            shared_score = sum(min(input_lower.count(ch), choice_lower.count(ch)) for ch in shared_letters)

            # Weighted total score
            return positional_score * 2 + shared_score  # tweak weights as needed

        # Score all choices
        scored = [(choice, score(choice)) for choice in choices]
        # Sort by score descending
        sorted_choices = sorted(scored, key=lambda x: x[1], reverse=True)
        # Return top matches
        return [choice for choice, _ in sorted_choices[:limit]]


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


    def get_all_data(reload_data, save_excel):
        # get date info
        today_str, last_day_prev_month_str = get_date_info()
        print("Last day previous month: ", last_day_prev_month_str)

        # load data
        df_bikes, df_bikes_descriptions, df_parts, df_accessories = Unleashed_get_all_product_forecast_data(
            start_date='2022-01-01',
            end_date=last_day_prev_month_str,
            reload=reload_data,
            save_excel=save_excel)

        df_sales = Unleashed_sales_forecast_data(start_date='2022-01-01', end_date=last_day_prev_month_str,
                                                 reload=reload_data, save_excel=save_excel)

        # ensure data is truncated at last day of last month
        df_bikes = df_bikes.loc[df_bikes['Year-Month'] <= last_day_prev_month_str]
        df_bikes_descriptions = df_bikes_descriptions.loc[df_bikes_descriptions['Year-Month'] <= last_day_prev_month_str]
        df_parts = df_parts.loc[df_parts['Year-Month'] <= last_day_prev_month_str]
        df_accessories = df_accessories.loc[df_accessories['Year-Month'] <= last_day_prev_month_str]
        df_sales = df_sales.loc[df_sales['Year-Month'] <= last_day_prev_month_str]

        return df_bikes, df_bikes_descriptions, df_parts, df_accessories, df_sales


    def get_bikes_product_data(reload_data, save_excel):
        today_str, last_day_prev_month_str = get_date_info()
        df_bikes, df_bikes_descriptions = Unleashed_bikes_product_forecast_data(start_date='2022-01-01',
                                                                                end_date=last_day_prev_month_str,
                                                                                reload=reload_data, save_excel=save_excel)

        df_bikes = df_bikes.loc[df_bikes['Year-Month'] <= last_day_prev_month_str]
        df_bikes_descriptions = df_bikes_descriptions.loc[df_bikes_descriptions['Year-Month'] <= last_day_prev_month_str]

        return df_bikes, df_bikes_descriptions




    df_warehouses_names = Unleashed_Warehouses_clean_data_parallel(reload=False, save_excel=True)['WarehouseName']

    load_all_data_input = input("Would you like to reload all product data? (y/n): ")
    if load_all_data_input == "y":
        print("Reloading all product data (this could take a few minutes)...")
        a, b, c, d, e = get_all_data(True, True)
        print("All product data reloaded!")

    reload_data = False
    save_excel = True

    special_bikes = ['Cosmo 2.0 T - Black- 48v 15 Ah', 'Cosmo 2.0 T - Calypso - 48v 15 Ah']

    while True:

        #load warehouses
        print("Product Category \"Bikes\" Chosen.")
        df_bikes, df_bikes_descriptions = get_bikes_product_data(reload_data=reload_data, save_excel=save_excel)
        bike_category = input("Enter bike category Specific SKU or Bike Type (s/b): ")


        # Display warehouse options with an added 'None' choice
        print(df_warehouses_names)
        print(f"{len(df_warehouses_names)}: None")  # Append 'None' option at the end
        index = int(input("Enter warehouse index number (or choose 'None'): "))
        if index == len(df_warehouses_names):
            warehouse_name = None
        else:
            warehouse_name = df_warehouses_names.loc[index]
        print("Chosen warehouse:", warehouse_name if warehouse_name else "None selected")


        if bike_category == 's':  # go into specific bike SKU

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
                # Cosmo black
                if product_name == 'Cosmo 2.0 T - Black- 48v 15 Ah':
                    print("Cosmo Black Hit")
                    cosmo_black_bike = df_bikes_descriptions.loc[
                        df_bikes_descriptions['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah'].reset_index(
                        drop=True)
                    lowrider_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions[
                                                                        'ProductDescription'] == 'Low rider 2.0 - Black-Copper - 48v 15Ah'].reset_index(
                        drop=True)
                    dash_cosmo_black_bike_launch(cosmo_black_bike=cosmo_black_bike,
                                                 lowrider_black_bike=lowrider_black_bike, product_name=product_name,
                                                 value_string=value_string, path=path,
                                                 forecast_horizon=forecast_horizon, warehouse_name=warehouse_name)

                # Cosmo calypso
                elif product_name == 'Cosmo 2.0 T - Calypso - 48v 15 Ah':
                    print("Cosmo Calypso Hit")
                    scaler = 0.15
                    cosmo_black_bike = df_bikes_descriptions.loc[
                        df_bikes_descriptions['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah'].reset_index(
                        drop=True)
                    lowrider_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions[
                                                                        'ProductDescription'] == 'Low rider 2.0 - Black-Copper - 48v 15Ah'].reset_index(
                        drop=True)
                    # adjust data
                    cosmo_black_bike['OrderQuantity'] = scaler * cosmo_black_bike['OrderQuantity']
                    lowrider_black_bike['OrderQuantity'] = scaler * lowrider_black_bike['OrderQuantity']

                    dash_cosmo_calypso_bike_launch(cosmo_black_bike=cosmo_black_bike,
                                                   lowrider_black_bike=lowrider_black_bike, product_name=product_name,
                                                   value_string=value_string, path=path,
                                                   forecast_horizon=forecast_horizon, warehouse_name=warehouse_name)

                #go into ibd bikes
                elif product_name in ['Wave - Graphite - 48V 15A', 'Wave - Ocean - 48V 15A', 'Wave - Pearl - 48V 15A', 'Wave - Chroma Green - 48V 15A',
                                      'Bliss - Amethyst - 48V 15A', 'Bliss - Chroma Green - 48V 15A', 'Bliss - Graphite - 48V 15A', 'Bliss - Wildfire - 48V 15A',
                                      'Edge - Graphite - 48V 15A', 'Edge - Ruby - 48V 15A', 'Edge - Stealth Camo - 48V 15A']:
                    print("Hit NPI")
                    analytical_forecast = int(input("Enter analytical forecast for IBD bike: "))
                    product_series = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == product_name].reset_index(drop=True)
                    dash_ibd_bike_launch(series=product_series, analytical_forecast=analytical_forecast, financial_forecast=poll_forecast,
                                     product_name=product_name,
                                     value_string=value_string, retrain=retrain, path=path,
                                     forecast_horizon=forecast_horizon,
                                     warehouse_name=warehouse_name)
                # Other bikes
                else:

                    product_series = df_bikes_descriptions.loc[
                        df_bikes_descriptions['ProductDescription'] == product_name].reset_index(drop=True)
                    dash_bike_launch(series=product_series, financial_forecast=poll_forecast, product_name=product_name,
                                     value_string=value_string, retrain=retrain, path=path,
                                     forecast_horizon=forecast_horizon, SKU_or_type='ProductDescription', warehouse_name=warehouse_name)

            else:
                df = reload_df_for_dash(product_name=product_name, path=path)
                dash_reload(df, product_name, path)

        else:  # go into specific bike type
            user_input = input("Enter bike type name or partial name: ")
            bike_names = df_bikes['Bike_type'].unique().tolist()
            product_name = display_matches(user_input=user_input, choices=bike_names)
            reload_dash_app = input("Start brand new dash app? (y/n): ")
            path = os.path.join("Product_Forecasting", "Bikes", "Bike_types")

            if reload_dash_app == "y":  # go into dash app
                value_string = 'OrderQuantity'
                retrain = False

                poll_forecast = [0, 0, 0, 0, 0, 0]
                forecast_horizon = 6

                product_series = df_bikes.loc[df_bikes['Bike_type'] == product_name].reset_index(drop=True)
                dash_bike_launch(series=product_series, financial_forecast=poll_forecast, product_name=product_name,
                                 value_string=value_string, retrain=retrain, path=path,
                                 forecast_horizon=forecast_horizon, SKU_or_type='Bike_type', warehouse_name=warehouse_name)

            else:
                df = reload_df_for_dash(product_name=product_name, path=path)
                dash_reload(df, product_name, path)




