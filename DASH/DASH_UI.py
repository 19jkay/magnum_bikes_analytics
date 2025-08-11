from rapidfuzz import process

from Product_Forecasting.Product_Forecast_Clean import *
from DASH.DASH_main import dash_bike_launch, dash_bike_reload
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info


def find_best_matches(user_input, choices, limit=7, threshold=60):
    """
    Returns the top matches for user_input from choices using fuzzy matching.
    """
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






# print(df_bikes_descriptions)
# print(df_bikes_descriptions['ProductDescription'].unique().tolist())
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
    product_series = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == product_name].reset_index(drop=True)

    dash_bike_launch(series=product_series, financial_forecast=poll_forecast, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)

else:
    path_segment = 'Product_Forecast'
    filename_suffix = "consensus_data"
    extension = "xlsx"

    # Build dynamic output path
    output_dir = os.path.join(r"C:\Users\joshu\Documents\DASH", path_segment)
    # os.makedirs(output_dir, exist_ok=True)

    # Clean filename components
    safe_product_name = product_name.replace("/", "_").replace(":", "_")
    filename = f"{safe_product_name}_{filename_suffix}.{extension}"

    # Full save path
    full_path = os.path.join(output_dir, filename)

    df = pd.read_excel(full_path)

    dash_bike_reload(df, product_name)

