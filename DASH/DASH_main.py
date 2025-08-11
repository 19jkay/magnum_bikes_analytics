from DASH.DASH_App import dash_app
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info
from Product_Forecasting.Product_Forecasting_Algorithm import forecast

import pandas as pd
import numpy as np
from datetime import datetime

def cosmo_dash(cosmo_black_forecast_series, poll_forecast, product_name):
    import matplotlib
    matplotlib.use('Agg')

    today_str, last_day_prev_month_str = get_date_info()

    start_date = cosmo_black_forecast_series.start_time().strftime('%Y-%m-%d')
    end_date = cosmo_black_forecast_series.end_time().strftime('%Y-%m-%d')
    dates = pd.date_range(start=start_date, end=end_date, freq='MS')  # 'MS' = Month Start

    num_dates = len(dates)


    #get stock on hand for cosmo black and calypso
    df_stockonhand = get_data_parallel(unleashed_data_name="StockOnHand", end_date=today_str)

    # df_stockonhand_cosmo = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah')
    #                                           | (df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Calypso - 48v 15 Ah')]
    # df_stockonhand_cosmo = df_stockonhand_cosmo[['ProductDescription', 'QtyOnHand']]
    #
    #
    # #get qtyonhand number for cosmo black
    # inventory_cosmo_black = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah']['QtyOnHand'].iloc[0]
    # inventory_cosmo_calypso = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == 'Cosmo 2.0 T - Calypso - 48v 15 Ah']['QtyOnHand'].iloc[0]

    df_stockonhand_cosmo = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    df_stockonhand_cosmo = df_stockonhand_cosmo[['ProductDescription', 'QtyOnHand']]
    inventory_specific_cosmo = df_stockonhand_cosmo.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]


    #get blank cosmo inventory lists
    cosmo_specific_inventory_list = [float(inventory_specific_cosmo)] + [0 for i in range(num_dates - 1)]

    # Build the DataFrame
    cosmo_black_data = pd.DataFrame({
        'Year-Month': dates,
        'Analytical Forecast (Kay)': [100] * 6,
        'Financial Forecast (Poll)': poll_forecast,
        'Inventory': cosmo_specific_inventory_list,
        'Ending Inventory': [0, 0, 0, 0, 0, 0],
        'Purchases' :  [0, 0, 0, 0, 0, 0]
    })

    cosmo_black_data['Analytical Forecast (Kay)'] = np.round(cosmo_black_forecast_series.univariate_values(), 2).tolist()
    cosmo_black_data['Final Consensus'] = 1 / 2 * (cosmo_black_data['Analytical Forecast (Kay)'] + cosmo_black_data['Financial Forecast (Poll)'])

    dash_app(cosmo_black_data, product_name)


def dash_bike_launch(series, financial_forecast, product_name, value_string, retrain, path, forecast_horizon):
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

    df_stockonhand_product = df_stockonhand.loc[(df_stockonhand['ProductDescription'] == product_name)]
    df_stockonhand_product = df_stockonhand_product[['ProductDescription', 'QtyOnHand']]
    inventory_specific_product = df_stockonhand_product.loc[df_stockonhand['ProductDescription'] == product_name]['QtyOnHand'].iloc[0]

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

    dash_app(data, product_name)

def dash_bike_reload(data, product_name):
    dash_app(data, product_name)
    return
