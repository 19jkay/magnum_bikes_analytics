import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

from darts.models import ExponentialSmoothing
from darts.models import Prophet
from darts.dataprocessing.transformers import BoxCox
from darts.metrics import mape
from darts import TimeSeries
from darts.utils.utils import SeasonalityMode, ModelMode, TrendMode


from Sales_Forecasting.Sales_Forecasting_Clean import *
from Sales_Forecasting.Sales_Forecasting_Algorithm import *

reload_data = False
df = Unleashed_sales_forecast_data(start_date='2022-01-01', end_date='2025-06-30', reload=reload_data)


RETRAIN = False

customer_types = ['Wholesale', 'Retail']
for customer_type in customer_types:
    one_customer_type = df.loc[df['CustomerType'] == customer_type].reset_index(drop=True)
    forecast(one_customer_type, customer_type=customer_type, value_string='LineTotal', retrain=RETRAIN, path=customer_type)


#
# #historical data
# total_df = df[['Year-Month', 'Sub Total']].reset_index(drop=True)
# total_df = total_df.groupby("Year-Month", as_index=False)['Sub Total'].sum()
#
# total_series = TimeSeries.from_dataframe(total_df, time_col="Year-Month", value_cols="Sub Total")
#
# df_B2B = df.loc[df['Customer Type'] == 'B2B']
# df_distributor = df.loc[df['Customer Type'] == 'Distributor']
# df_employee = df.loc[df['Customer Type'] == 'Employee']
# df_costco = df.loc[df['Customer Type'] == 'Costco']
# df_wholesale = df.loc[df['Customer Type'] == 'Wholesale']
# df_web = df.loc[df['Customer Type'] == 'Web']
# df_retail = df.loc[df['Customer Type'] == 'Retail']
#
#
# #naive seasonal forecasts
# B2B_seasonal_forecast = B2B_Seasonal(df_B2B)
# distributor_forecast = distributor(df_distributor)
# employee_forecast = employee(df_employee)
#
# #complex forecasts
# costco_forecast, costco_flow_plan = costco(df_costco)
# costco_complete_forecast = costco_flow_plan.append(costco_forecast[2:])
#
# wholesale_forecast, wholesale_ci_forecast = wholesale(df_wholesale)
# wholesale_forecast = wholesale_forecast[1:]
#
# web_forecast, web_ci_forecast = web(df_web)
# web_forecast = web_forecast[1:]
#
# retail_forecast, retail_ci_forecast = retail(df_retail)
# retail_forecast = retail_forecast[1:]
#
# total_forecast = B2B_seasonal_forecast + distributor_forecast + employee_forecast + costco_complete_forecast + wholesale_forecast + web_forecast + retail_forecast
#
# print(total_forecast)
#
# # plot forecasts
# total_series.plot(label="Historical Data")
# total_forecast.plot(label="Forecast")
# plt.legend()
# plt.xlabel('Year-Month')
# plt.ylabel('Sales ($)')
# plt.title("Total Sales Forecast")
#
# output_dir = r"C:\Users\joshu\Documents\Sales_Forecast_Customer_Type"
# os.makedirs(output_dir, exist_ok=True)
# customer_type = "Total_Sales"
# filename = f"{customer_type.replace('/', '_').replace(':', '_')}.png"
# filepath = os.path.join(output_dir, filename)
# plt.savefig(filepath, dpi=300)
# plt.show()
# plt.close()
