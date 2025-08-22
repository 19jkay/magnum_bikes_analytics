from DASH.DASH_App import dash_app
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info
from Product_Forecasting.Product_Forecasting_Algorithm import forecast
from Product_Forecasting.Costco_Product_Forecasting_Algorithm import cosmo_black_forecast, cosmo_calypso_forecast

import pandas as pd
import numpy as np
from datetime import datetime

def cosmo_dash(cosmo_black_forecast_series, poll_forecast, final_consensus, inventory, purchases, dates, product_name):
    import matplotlib
    matplotlib.use('Agg')

    # today_str, last_day_prev_month_str = get_date_info()
    #
    # start_date = cosmo_black_forecast_series.start_time().strftime('%Y-%m-%d')
    # end_date = cosmo_black_forecast_series.end_time().strftime('%Y-%m-%d')
    # dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start
    # print("Dates: ", dates)
    #
    # num_dates = len(dates)


    #get stock on hand for cosmo black and calypso
    # df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

    # df_stockonhand_cosmo = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah')
    #                                           | (df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Calypso - 48v 15 Ah')]
    # df_stockonhand_cosmo = df_stockonhand_cosmo[['ProductDescription', 'QtyOnHand']]
    #
    #
    # #get qtyonhand number for cosmo black
    # inventory_cosmo_black = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah']['QtyOnHand'].iloc[0]
    # inventory_cosmo_calypso = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Calypso - 48v 15 Ah']['QtyOnHand'].iloc[0]

    # df_stockonhand_cosmo = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    # df_stockonhand_cosmo = df_stockonhand_cosmo[['ProductDescription', 'QtyOnHand']]
    # inventory_specific_cosmo = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]
    #

    #get blank cosmo inventory lists
    # cosmo_specific_inventory_list = [float(inventory_specific_cosmo)] + [0 for i in range(num_dates - 1)]

    # Build the DataFrame
    cosmo_black_data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': cosmo_black_forecast_series,
        'Financial Forecast (Poll)': poll_forecast,
        'Final Consensus' : final_consensus,
        'Inventory': inventory,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases' :  purchases
    })

    # cosmo_black_data['Analytical Forecast (Kay)'] = np.round(cosmo_black_forecast_series.univariate_values(), 2).tolist()
    # cosmo_black_data['Final Consensus'] = 1 / 2 * (cosmo_black_data['Analytical Forecast (Kay)'] + cosmo_black_data['Financial Forecast (Poll)'])

    dash_app(cosmo_black_data, product_name)


def dash_bike_launch(series, financial_forecast, product_name, value_string, retrain, path, forecast_horizon, SKU_or_type):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    #get forecast
    series_forecast, series_forecast_ci = forecast(series, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)
    #get forecast dates
    start_date = series_forecast.start_time().strftime('%Y-%m-%d')
    end_date = series_forecast.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start
    num_dates = len(dates)

    #get StockOnHand (write code in future to just get stock for product_name, need GUID)
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)


    # df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    # df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]
    # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]

    df_stockonhand_product = df_stockonhand.loc[(df_stockonhand[SKU_or_type] == product_name)]
    df_stockonhand_product = df_stockonhand_product[[SKU_or_type, 'QtyOnHand']]

    if SKU_or_type == 'Bike_type':
        df_stockonhand_product = df_stockonhand_product.groupby(SKU_or_type)['QtyOnHand'].sum().reset_index()

    # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand[SKU_or_type] == product_name]['QtyOnHand'].iloc[0]
    inventory_specific_product = df_stockonhand_product['QtyOnHand'].iloc[0]

    # get blank cosmo inventory lists
    inventory_specific_product_list = [float(inventory_specific_product)] + [0 for i in range(num_dates - 1)]

    analytical_forecast = np.round(series_forecast.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': inventory_specific_product_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    #add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)


def dash_cosmo_black_bike_launch(cosmo_black_bike, lowrider_black_bike, product_name, value_string, path, forecast_horizon):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    # get stock on hand for cosmo black and calypso
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)


    cosmo_black_forecast_series = cosmo_black_forecast(cosmo_black_bike, lowrider_black_bike, product_name=product_name, value_string=value_string, path=path, forecast_horizon=forecast_horizon)

    print("cosmo_black_forecast_series: ", cosmo_black_forecast_series)

    df_stockonhand_cosmo = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    df_stockonhand_cosmo = df_stockonhand_cosmo[['ProductDescription', 'QtyOnHand']]
    inventory_specific_cosmo = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]


    start_date = cosmo_black_forecast_series.start_time().strftime('%Y-%m-%d')
    end_date = cosmo_black_forecast_series.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start

    num_dates = len(dates)
    cosmo_specific_inventory_list = [float(inventory_specific_cosmo)] + [0 for _ in range(num_dates - 1)]

    financial_forecast = [700, 1300, 1400, 1400, 1000, 0]
    analytical_forecast = np.round(cosmo_black_forecast_series.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': cosmo_specific_inventory_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    # add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)

def dash_cosmo_calypso_bike_launch(cosmo_black_bike, lowrider_black_bike, product_name, value_string, path, forecast_horizon):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    # get stock on hand for cosmo black and calypso
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

    cosmo_calypso_forecast_series = cosmo_calypso_forecast(cosmo_black_bike, lowrider_black_bike, product_name=product_name, value_string=value_string, path=path, forecast_horizon=forecast_horizon)

    print("cosmo_black_forecast_series: ", cosmo_calypso_forecast_series)

    df_stockonhand_cosmo = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    df_stockonhand_cosmo = df_stockonhand_cosmo[['ProductDescription', 'QtyOnHand']]
    inventory_specific_cosmo = \
    df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]

    start_date = cosmo_calypso_forecast_series.start_time().strftime('%Y-%m-%d')
    end_date = cosmo_calypso_forecast_series.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start

    num_dates = len(dates)
    cosmo_specific_inventory_list = [float(inventory_specific_cosmo)] + [0 for _ in range(num_dates - 1)]

    financial_forecast = [200, 200, 300, 400, 300, 0]
    analytical_forecast = np.round(cosmo_calypso_forecast_series.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': cosmo_specific_inventory_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    # add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)





def dash_parts_launch(series, financial_forecast, product_name, value_string, retrain, path, forecast_horizon):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    #get forecast
    series_forecast, series_forecast_ci = forecast(series, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)
    #get forecast dates
    start_date = series_forecast.start_time().strftime('%Y-%m-%d')
    end_date = series_forecast.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start
    num_dates = len(dates)

    #get StockOnHand (write code in future to just get stock for product_name, need GUID)
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)


    # df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    # df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]
    # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]

    df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]

    # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand[SKU_or_type] == product_name]['QtyOnHand'].iloc[0]
    inventory_specific_product = df_stockonhand_product['QtyOnHand'].iloc[0]

    # get blank cosmo inventory lists
    inventory_specific_product_list = [float(inventory_specific_product)] + [0 for i in range(num_dates - 1)]

    analytical_forecast = np.round(series_forecast.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': inventory_specific_product_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    #add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)


def dash_parts_other_launch(series, financial_forecast, product_name, value_string, retrain, path, forecast_horizon, top_parts):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    #get forecast
    series_forecast, series_forecast_ci = forecast(series, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)
    #get forecast dates
    start_date = series_forecast.start_time().strftime('%Y-%m-%d')
    end_date = series_forecast.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start
    num_dates = len(dates)

    #get StockOnHand (write code in future to just get stock for product_name, need GUID)
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

    parts_groups = ['Battery',
                    'Bottom Brackets', 'Brakes', 'Chargers',
                    'Cockpit', 'Controllers', 'Conversion Kit', 'Derailleur Hangers',
                    'Displays', 'Drivetrain', 'Electronics', 'Fenders', 'Forks', 'Frame',
                    'Headset', 'Lights', 'Motor Wheels', 'Motors',
                    'Racks', 'Scooters', 'Shifters', 'Throttles', 'Tires', 'Tubes',
                    'Wheels', 'Derailleurs']

    df_stockonhand = df_stockonhand.loc[df_stockonhand['ProductGroupName'].isin(parts_groups)]
    df_stockonhand = df_stockonhand.loc[~df_stockonhand['ProductDescription'].isin(top_parts)]
    print("before: ", df_stockonhand)
    inventory_parts_other = df_stockonhand['QtyOnHand'].sum()



    # df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    # df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]
    #
    # # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand[SKU_or_type] == product_name]['QtyOnHand'].iloc[0]
    # inventory_specific_product = df_stockonhand_product['QtyOnHand'].iloc[0]

    # get blank cosmo inventory lists
    inventory_specific_product_list = [float(inventory_parts_other)] + [0 for i in range(num_dates - 1)]

    analytical_forecast = np.round(series_forecast.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': inventory_specific_product_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    #add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)



def dash_accessories_launch(series, financial_forecast, product_name, value_string, retrain, path, forecast_horizon):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    #get forecast
    series_forecast, series_forecast_ci = forecast(series, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)
    #get forecast dates
    start_date = series_forecast.start_time().strftime('%Y-%m-%d')
    end_date = series_forecast.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start
    num_dates = len(dates)

    #get StockOnHand (write code in future to just get stock for product_name, need GUID)
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

    #go over
    # df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    # df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]
    # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]

    df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]

    # inventory_specific_product = df_stockonhand_product.loc[df_stockonhand[SKU_or_type] == product_name]['QtyOnHand'].iloc[0]
    inventory_specific_product = df_stockonhand_product['QtyOnHand'].iloc[0]

    # get blank cosmo inventory lists
    inventory_specific_product_list = [float(inventory_specific_product)] + [0 for i in range(num_dates - 1)]

    analytical_forecast = np.round(series_forecast.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': inventory_specific_product_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    #add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)



def dash_accessories_other_launch(series, financial_forecast, product_name, value_string, retrain, path, forecast_horizon, top_accessories):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    #get forecast
    series_forecast, series_forecast_ci = forecast(series, product_name=product_name, value_string=value_string, retrain=retrain, path=path, forecast_horizon=forecast_horizon)
    #get forecast dates
    start_date = series_forecast.start_time().strftime('%Y-%m-%d')
    end_date = series_forecast.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start
    num_dates = len(dates)

    #get StockOnHand (write code in future to just get stock for product_name, need GUID)
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)


    df_stockonhand = df_stockonhand.loc[(df_stockonhand['ProductGroupName'] == 'Accessories') | (df_stockonhand['ProductGroupName'] == 'Accessories SLC Store')]
    df_stockonhand = df_stockonhand.loc[~df_stockonhand['ProductDescription'].isin(top_accessories)]

    inventory_accessories_other = df_stockonhand['QtyOnHand'].sum()


    # get blank cosmo inventory lists
    inventory_specific_product_list = [float(inventory_accessories_other)] + [0 for i in range(num_dates - 1)]

    analytical_forecast = np.round(series_forecast.univariate_values(), 2).tolist()

    # Fill the DataFrame
    data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': analytical_forecast,
        'Financial Forecast (Poll)': financial_forecast,
        'Inventory': inventory_specific_product_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases': [0, 0, 0, 0, 0, 0]
    })

    #add final consensus column that is average of forecasts
    data['Final Consensus'] = 1 / 2 * (data['Analytical Forecast (Kay)'] + data['Financial Forecast (Poll)'])

    dash_app(data, product_name, path)


def dash_reload(data, product_name, path):
    dash_app(data, product_name, path)
    return
