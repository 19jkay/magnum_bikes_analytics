import pandas as pd
import calendar
from datetime import datetime, date

from Product_Forecasting.Product_Forecast_Clean import *
from Product_Forecasting.Product_Forecasting_Algorithm import *
from Product_Forecasting.Costco_Product_Forecasting_Algorithm import *
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info
from DASH.DASH_main import *


#get date info
today_str, last_day_prev_month_str = get_date_info()
print("Today:", today_str)
print("Last day of previous month:", last_day_prev_month_str)

#load data
reload_data = False
save_excel = False
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


#retrain models?
RETRAIN = False


# print("here")
# #Cosmo 2.0 black Forecast
cosmo_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == 'Cosmo 2.0 T - Black- 48v 15 Ah'].reset_index(drop=True)
lowrider_black_bike = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == 'Low rider 2.0 - Black-Copper - 48v 15Ah'].reset_index(drop=True)
poll_cosmo_black_forecast = [700, 1300, 1400, 1400, 1000, 0]
cosmo_black_forecast_series = cosmo_black_forecast(cosmo_black_bike, lowrider_black_bike,  product_name='Cosmo 2.0 T - Black- 48v 15 Ah', value_string='OrderQuantity', path='Bike_Descriptions')

cosmo_dash(cosmo_black_forecast_series, poll_forecast=poll_cosmo_black_forecast, product_name='Cosmo 2.0 T - Black- 48v 15 Ah') #Dash App



adjusted_cosmo_black_bike = cosmo_black_bike
adjusted_lowrider_black_bike = lowrider_black_bike
scaler = 0.15
adjusted_cosmo_black_bike['OrderQuantity'] = scaler * adjusted_cosmo_black_bike['OrderQuantity']
adjusted_lowrider_black_bike['OrderQuantity'] = scaler * adjusted_lowrider_black_bike['OrderQuantity']
poll_cosmo_calypso_forecast = [200, 200, 300, 400, 300, 0]
cosmo_calypso_forecast_series = cosmo_calypso_forecast(adjusted_cosmo_black_bike, adjusted_lowrider_black_bike,  product_name='Cosmo 2.0 T - Calypso - 48v 15 Ah', value_string='OrderQuantity', path='Bike_Descriptions')

cosmo_dash(cosmo_calypso_forecast_series,poll_forecast=poll_cosmo_calypso_forecast, product_name='Cosmo 2.0 T - Calypso - 48v 15 Ah') #Dash App

print("Completed")

