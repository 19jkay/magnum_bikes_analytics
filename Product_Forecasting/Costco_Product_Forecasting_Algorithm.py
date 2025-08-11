import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import re


from darts.models import Prophet
from darts.dataprocessing.transformers import BoxCox
from darts.metrics import mape
from darts import TimeSeries
from darts.models import NaiveEnsembleModel, NaiveSeasonal, LinearRegressionModel, NaiveMovingAverage
import joblib
import importlib

from Product_Forecasting.Bootstrap_Prediction_Intervals.Bootstrap import *

def sanitize_path_component(component):
    # Keep alphanumerics, underscores, dashes, and spaces; replace others with underscore
    return re.sub(r'[<>:"/\\|?*]', '_', component)

def lowrider_forecast(df_lowrider, product_name,  value_string, path, forecast_horizon=6):
    print("Running: ", product_name)

    safe_product_name = sanitize_path_component(product_name)  # make sure name is in safe format for path


    df_lowrider = df_lowrider[['Year-Month', value_string]].reset_index(drop=True)
    df_lowrider[value_string] = df_lowrider[value_string].clip(lower=0)  # make sure smallest value is 0


    # remove beginning months that are zero if bike has not come out yet
    first_nonzero_index = df_lowrider[df_lowrider['OrderQuantity'] != 0].index.min()
    df_lowrider = df_lowrider.loc[first_nonzero_index:].reset_index(drop=True)

    #get only data after may 2024
    df_lowrider = df_lowrider.loc[df_lowrider['Year-Month'] >= '2024-06-01']

    # convert to darts series and add 2 so boxcox transformation can be used.
    series_lowrider = TimeSeries.from_dataframe(df_lowrider, "Year-Month", value_string, freq='MS')


    #Remember to boxcox transform
    # transformer = BoxCox()
    # transformer.fit_transform(series_lowrider)
    # transformed_series_lowrider = transformer.transform(series_lowrider)
    # a, b, c = STL_try(transformed_series_lowrider)

    model = NaiveEnsembleModel([NaiveSeasonal(K=12), LinearRegressionModel(lags=4)])
    model.fit(series_lowrider)
    forecast = model.predict(forecast_horizon)
    model_name = 'NaiveEnsemble'

    # plot forecasts
    series_lowrider.plot()
    forecast.plot(label="Forecast")
    plt.legend()
    plt.xlabel('Year-Month')
    plt.ylabel(value_string)
    plt.title(product_name + " Forecast using " + model_name)

    # output_dir = r"C:\Users\joshu\Documents\Product_Forecast\" + path
    output_dir = os.path.join(r"C:\Users\joshu\Documents\Product_Forecast", path)
    os.makedirs(output_dir, exist_ok=True)
    customer_type = path + '_' + safe_product_name
    filename = f"{customer_type.replace('/', '_').replace(':', '_')}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=300)
    plt.close()

    return forecast


def cosmo_black_forecast(df_cosmo, df_lowrider,  product_name, value_string, path, forecast_horizon=6):
    print("Running: ", product_name)

    safe_product_name = sanitize_path_component(product_name)  # make sure name is in safe format for path

    df_cosmo = df_cosmo[['Year-Month', value_string]].reset_index(drop=True)
    df_cosmo[value_string] = df_cosmo[value_string].clip(lower=0)  # make sure smallest value is 0

    df_lowrider = df_lowrider[['Year-Month', value_string]].reset_index(drop=True)
    df_lowrider[value_string] = df_lowrider[value_string].clip(lower=0)  # make sure smallest value is 0

    # # remove beginning months that are zero if bike has not come out yet
    first_nonzero_index = df_cosmo[df_cosmo['OrderQuantity'] != 0].index.min()
    df_cosmo = df_cosmo.loc[first_nonzero_index:].reset_index(drop=True)

    # remove beginning months that are zero if bike has not come out yet
    first_nonzero_index = df_lowrider[df_lowrider['OrderQuantity'] != 0].index.min()
    df_lowrider = df_lowrider.loc[first_nonzero_index:].reset_index(drop=True)

    first_nonzero_index = df_cosmo[df_cosmo['OrderQuantity'] != 0].index.min()
    df_cosmo = df_cosmo.loc[first_nonzero_index:].reset_index(drop=True)

    # get only data after may 2024 and before when Cosmo hit in may 2025
    df_lowrider = df_lowrider.loc[df_lowrider['Year-Month'] >= '2024-06-01']
    df_lowrider = df_lowrider.loc[df_lowrider['Year-Month'] <= '2025-04-01']

    #get cosmo data only after it really hits costco May 2025
    df_cosmo = df_cosmo.loc[df_cosmo['Year-Month'] > '2025-04-01']


    series_cosmo = TimeSeries.from_dataframe(df_cosmo, "Year-Month", value_string, freq='MS')
    series_lowrider = TimeSeries.from_dataframe(df_lowrider, "Year-Month", value_string, freq='MS')

    series = series_lowrider.append(series_cosmo)

    # block_size = 4
    # periods = 3
    # transformer = BoxCox()
    # transformer.fit_transform(series)
    # transformed_series = transformer.transform(series)
    # bootstrap_forecasts = bootstrap_prediction_interval(transformed_series, model_info, periods, block_size, forecast_horizon, value_string)
    # bootstrap_forecasts = transformer.inverse_transform(bootstrap_forecasts)


    model = NaiveSeasonal(K=12)
    model.fit(series)
    forecast = model.predict(forecast_horizon)
    model_name = 'NaiveSeasonal'

    # plot forecasts
    series_cosmo.plot(label='Cosmo 2.0 T - Black- 48v 15 Ah')
    series_lowrider.plot(label='Low rider 2.0 - Black-Copper - 48v 15Ah')
    forecast.plot(label=product_name)
    # bootstrap_forecasts.plot(label=product_name, low_quantile=0.05, high_quantile=0.95)
    plt.legend()
    plt.xlabel('Year-Month')
    plt.ylabel(value_string)
    plt.title(product_name + " Forecast using " + model_name)

    # output_dir = r"C:\Users\joshu\Documents\Product_Forecast\" + path
    output_dir = os.path.join(r"C:\Users\joshu\Documents\Product_Forecast", path)
    os.makedirs(output_dir, exist_ok=True)
    customer_type = path + '_' + safe_product_name
    filename = f"{customer_type.replace('/', '_').replace(':', '_')}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=300)
    plt.close()

    return forecast




def cosmo_calypso_forecast(df_cosmo, df_lowrider,  product_name, value_string, path, forecast_horizon=6):
    print("Running: ", product_name)

    safe_product_name = sanitize_path_component(product_name)  # make sure name is in safe format for path

    df_cosmo = df_cosmo[['Year-Month', value_string]].reset_index(drop=True)
    df_cosmo[value_string] = df_cosmo[value_string].clip(lower=0)  # make sure smallest value is 0

    df_lowrider = df_lowrider[['Year-Month', value_string]].reset_index(drop=True)
    df_lowrider[value_string] = df_lowrider[value_string].clip(lower=0)  # make sure smallest value is 0

    # # remove beginning months that are zero if bike has not come out yet
    first_nonzero_index = df_cosmo[df_cosmo['OrderQuantity'] != 0].index.min()
    df_cosmo = df_cosmo.loc[first_nonzero_index:].reset_index(drop=True)

    # remove beginning months that are zero if bike has not come out yet
    first_nonzero_index = df_lowrider[df_lowrider['OrderQuantity'] != 0].index.min()
    df_lowrider = df_lowrider.loc[first_nonzero_index:].reset_index(drop=True)

    first_nonzero_index = df_cosmo[df_cosmo['OrderQuantity'] != 0].index.min()
    df_cosmo = df_cosmo.loc[first_nonzero_index:].reset_index(drop=True)

    # get only data after may 2024 and before when Cosmo hit in may 2025
    df_lowrider = df_lowrider.loc[df_lowrider['Year-Month'] >= '2024-06-01']
    df_lowrider = df_lowrider.loc[df_lowrider['Year-Month'] <= '2025-04-01']

    #get cosmo data only after it really hits costco May 2025
    df_cosmo = df_cosmo.loc[df_cosmo['Year-Month'] > '2025-04-01']




    series_cosmo = TimeSeries.from_dataframe(df_cosmo, "Year-Month", value_string, freq='MS')
    series_lowrider = TimeSeries.from_dataframe(df_lowrider, "Year-Month", value_string, freq='MS')

    series = series_lowrider.append(series_cosmo)

    # transformer = BoxCox()
    # transformer.fit_transform(series)
    # transformed_series = transformer.transform(series)
    # trend, seasonal, residuals = STL_try(transformed_series)
    # boot_remainders = block_bootstrap(residuals, block_size=3, n_samples=1000)
    # boot_series = reconstruct_series(trend, seasonal, boot_remainders)

    # series_cosmo.plot()
    # series_lowrider.plot()
    # plt.show()

    model = NaiveSeasonal(K=12)
    model.fit(series)
    forecast = model.predict(forecast_horizon)
    model_name = 'NaiveSeasonal'

    # plot forecasts
    series_cosmo.plot(label='0.08 * Cosmo 2.0 T - Black- 48v 15 Ah')
    series_lowrider.plot(label='0.08 * Low rider 2.0 - Black-Copper - 48v 15Ah')
    forecast.plot(label=product_name)
    plt.legend()
    plt.xlabel('Year-Month')
    plt.ylabel(value_string)
    plt.title(product_name + " Forecast using " + model_name)

    # output_dir = r"C:\Users\joshu\Documents\Product_Forecast\" + path
    output_dir = os.path.join(r"C:\Users\joshu\Documents\Product_Forecast", path)
    os.makedirs(output_dir, exist_ok=True)
    customer_type = path + '_' + safe_product_name
    filename = f"{customer_type.replace('/', '_').replace(':', '_')}.png"
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=300)
    plt.close()

    return forecast

