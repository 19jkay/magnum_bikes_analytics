import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import re


from darts.models import Prophet
from darts.dataprocessing.transformers import BoxCox
from darts.metrics import mape
from darts import TimeSeries
from darts.models import NaiveEnsembleModel, NaiveSeasonal, LinearRegressionModel
import joblib
import importlib


def sanitize_path_component(component):
    # Keep alphanumerics, underscores, dashes, and spaces; replace others with underscore
    return re.sub(r'[<>:"/\\|?*]', '_', component)

def forecast(df, customer_type, value_string, path, retrain=False, forecast_horizon=6):

    safe_product_name = sanitize_path_component(customer_type)  # make sure name is in safe format for path

    print("Running: ", customer_type)
    # df = df[['Year-Month', 'Quantity']].reset_index(drop=True)
    df = df[['Year-Month', value_string]].reset_index(drop=True)

    df[value_string] = df[value_string].clip(lower=0) #make sure smallest value is 0

    #convert to darts series and add 2 so boxcox transformation can be used.
    series = TimeSeries.from_dataframe(df, "Year-Month", value_string) + 2

    train, test = series.split_before(pd.Timestamp('2025-05-01'))  # give two months of test data

    # Transform data
    transformer = BoxCox()
    transformer.fit_transform(train)
    transformed_series = transformer.transform(series)

    if retrain:

        transformed_train = transformer.transform(train)
        transformed_test = transformer.transform(test)

        print("Retraining...")
        # Navie ensemble model
        parameters = {"forecasting_models": [[NaiveSeasonal(K=12), LinearRegressionModel(lags=12)],
                                             [NaiveSeasonal(K=12), LinearRegressionModel(lags=8)],
                                             [NaiveSeasonal(K=12), LinearRegressionModel(lags=4)]]}

        best_model_ns, parameters_ns, score_ns = NaiveEnsembleModel.gridsearch(parameters=parameters,
                                                                               series=transformed_train,
                                                                               forecast_horizon=6,
                                                                               start=0.7,
                                                                               metric=mape,
                                                                               reduction=np.mean)

        best_model_ns.fit(transformed_train)
        forecast_ns_test = best_model_ns.predict(2)
        mape_score_ns = mape(transformed_test, forecast_ns_test)

        combined_ns_score = mape_score_ns


        #Prophet
        parameters = {"add_seasonalities": [{'name': "monthly_seasonality", 'seasonal_periods': 12, 'fourier_order': 5, 'mode': 'additive'},
                                            {'name': "monthly_seasonality", 'seasonal_periods': 12, 'fourier_order': 3, 'mode': 'multiplicative'}, ]}
        best_model_prophet, parameters_prophet, score_prophet = Prophet.gridsearch(parameters=parameters,
                                                                                   series=transformed_train,
                                                                                   forecast_horizon=6,
                                                                                   start=0.7,
                                                                                   metric=mape,
                                                                                   reduction=np.mean)

        best_model_prophet.fit(transformed_train)
        forecast_prophet_test = best_model_prophet.predict(2)

        mape_score_prophet = mape(transformed_test, forecast_prophet_test)

        combined_prophet_score = mape_score_prophet

        if combined_prophet_score <= combined_ns_score:
            best_model = best_model_prophet
            model_name = "Prophet"

        else:
            best_model = best_model_ns
            model_name = "NaiveSeasonal"


        print('NavieSeasonal Scores. Historical: ', score_ns, ' Test: ', mape_score_ns)
        print('Prophet Scores. Historical: ', score_prophet, ' Test: ', mape_score_prophet)
        print("Best Model: ", best_model)

        best_model.fit(transformed_series)
        forecast_transformed = best_model.predict(forecast_horizon)

        # cannot do prediction intervals for NaiveSeasonal
        if model_name == "NaiveSeasonal":
            forecast_ci_transformed = best_model.predict(forecast_horizon)
        else:
            forecast_ci_transformed = best_model.predict(forecast_horizon, num_samples=1000)


        # back transform forecasts and prediction
        forecast = transformer.inverse_transform(forecast_transformed) - 2
        forecast_ci = transformer.inverse_transform(forecast_ci_transformed) - 2
        series = series - 2

        # plot forecasts
        series.plot()
        forecast_ci.plot(label="Forecast", low_quantile=0.05, high_quantile=0.95)
        plt.legend()
        plt.xlabel('Year-Month')
        plt.ylabel(value_string)
        plt.title(customer_type + " Forecast using " + model_name)

        # output_dir = r"C:\Users\joshu\Documents\Product_Forecast\" + path
        output_dir = os.path.join(r"C:\Users\joshu\Documents\Sales_Forecast", path)
        os.makedirs(output_dir, exist_ok=True)
        customer_type = path + '_' + safe_product_name
        filename = f"{customer_type.replace('/', '_').replace(':', '_')}.png"
        filepath = os.path.join(output_dir, filename)
        plt.savefig(filepath, dpi=300)
        plt.close()



        # Define output directory and filename
        output_dir_model = os.path.join(r"C:\Users\joshu\Documents\Sales_Forecast", path, "models")
        os.makedirs(output_dir_model, exist_ok=True)

        filename = f"{customer_type.replace('/', '_').replace(':', '_')}.pkl"
        model_path = os.path.join(output_dir_model, filename)

        # Save the trained model
        best_model.save(model_path)

        # Save metadata to help reload later
        model_info = {
            "class_name": best_model.__class__.__name__,
            "file_path": model_path
        }
        info_path = model_path.replace(".pkl", "_info.pkl")
        joblib.dump(model_info, info_path)

    else:
        print("Loading...")

        # Construct the file path
        output_dir_model = os.path.join(r"C:\Users\joshu\Documents\Sales_Forecast", path, "models")
        customer_type = path + '_' + safe_product_name
        filename = f"{customer_type.replace('/', '_').replace(':', '_')}.pkl"
        filepath = os.path.join(output_dir_model, filename)

        # Load metadata
        info_path = os.path.join(output_dir_model, filepath.replace(".pkl", "_info.pkl"))
        model_info = joblib.load(info_path)

        # Dynamically load the correct model class
        model_class_name = model_info["class_name"]
        module = importlib.import_module("darts.models")
        model_class = getattr(module, model_class_name)

        # Load the trained model
        loaded_model = model_class.load(model_info["file_path"])


        forecast_transformed = loaded_model.predict(forecast_horizon)

        model_name = "NaiveSeasonal"
        # cannot do prediction intervals for NaiveSeasonal
        if model_name == "NaiveSeasonal":
            forecast_ci_transformed = loaded_model.predict(forecast_horizon)
        else:
            forecast_ci_transformed = loaded_model.predict(forecast_horizon, num_samples=1000)

        # back transform forecasts and prediction
        forecast = transformer.inverse_transform(forecast_transformed) - 2
        forecast_ci = transformer.inverse_transform(forecast_ci_transformed) - 2
        series = series - 2

        series.plot()
        plt.xlabel('Year-Month')
        plt.ylabel(value_string)
        plt.title(path + " Historical Data")
        plt.show()

    return forecast, forecast_ci