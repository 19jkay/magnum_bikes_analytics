import matplotlib.pyplot as plt
from darts.utils.statistics import extract_trend_and_seasonality
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from darts.models import NaiveSeasonal
from darts import TimeSeries
from darts.dataprocessing.transformers import BoxCox
import pandas as pd
import importlib

from darts.utils.utils import ModelMode



def decompose_series(series, periods):
    pd_series = series.to_series()
    stl = STL(pd_series, period=periods)
    result = stl.fit()

    trend = result.trend
    seasonal = result.seasonal
    residuals = result.resid

    #plotting
    # trend.plot(label='trend')
    # seasonal.plot(label='seasonal')
    # residuals.plot(label='residuals')
    # pd_series.plot(label='series')
    # plt.legend()
    # plt.show()
    return trend, seasonal, residuals



def block_bootstrap(remainder, block_size, n_samples):
    n = len(remainder)
    base_blocks = n // block_size
    remainder_points = n % block_size
    bootstraps = []

    for _ in range(n_samples):
        # Sample regular blocks
        indices = np.random.randint(0, n - block_size + 1, base_blocks)
        blocks = [remainder[i:i+block_size] for i in indices]

        # Handle excess block
        if remainder_points > 0:
            # Choose a valid excess block start location randomly
            excess_start = np.random.randint(0, n - remainder_points + 1)
            excess_block = remainder[excess_start:excess_start + remainder_points]

            # Randomly insert excess block into existing list of blocks
            insert_idx = np.random.randint(0, len(blocks) + 1)
            blocks.insert(insert_idx, excess_block)

        # Concatenate and trim to original length
        bootstrapped_series = np.concatenate(blocks)[:n]
        bootstraps.append(bootstrapped_series)

    return bootstraps



def reconstruct_series(trend, seasonal, boot_remainders):
    boot_series = []
    for rem in boot_remainders:
        reconstructed = trend + seasonal + rem
        boot_series.append(reconstructed)
    return boot_series


def bootstrap_prediction_interval(transformed_series, model_info, periods, block_size, forecast_horizon, filepath):

    #bootstrapping

    trend, seasonal, remainder = decompose_series(transformed_series, periods)
    boot_remainders = block_bootstrap(remainder, block_size=block_size, n_samples=1000)
    series_list = reconstruct_series(trend, seasonal, boot_remainders)

    # Dynamically load the correct model class
    model_class_name = model_info["class_name"]
    module = importlib.import_module("darts.models")
    model_class = getattr(module, model_class_name)

    # Load the trained model
    model = model_class.load(filepath)

    forecasts = []
    for data in series_list:
        series_data = TimeSeries.from_values(data)
        # model = NaiveSeasonal(K=12)
        model.fit(series_data)
        future = model.predict(forecast_horizon)
        forecasts.append(future.values().flatten())

    #dummy forecast to get forecast dates
    model = NaiveSeasonal(K=12)
    model.fit(transformed_series)
    forecast_dates = model.predict(forecast_horizon)

    forecasts_np = np.array(forecasts)
    forecasts_np = forecasts_np.T[:, np.newaxis, :] #get data into (6, 1, 1000)
    time_index = forecast_dates.time_index
    forecasts_series = TimeSeries.from_times_and_values(time_index, forecasts_np)
    bootstrap_forecasts = forecasts_series

    return bootstrap_forecasts
