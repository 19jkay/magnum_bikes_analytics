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
RETRAIN = True


bike_descriptions = df_bikes_descriptions['ProductDescription'].unique()

for bike_description_type in bike_descriptions:
    one_bike_description_type = df_bikes_descriptions.loc[df_bikes_descriptions['ProductDescription'] == bike_description_type].reset_index(drop=True)
    forecast(one_bike_description_type, product_name=bike_description_type, value_string='OrderQuantity', retrain=RETRAIN, path='Bike_Descriptions')




#PARTS
forecast_parts, forecast_ci_parts = forecast(df_parts, product_name='parts', value_string='OrderQuantity', retrain=RETRAIN, path='Parts')
df_forecast_parts = forecast_parts.to_dataframe().reset_index()






# # #attention bikes
# ibd_bikes = ['Bliss', 'Edge', 'Wave']
# for bike_type in ibd_bikes:
#     one_bike_type = df_bikes.loc[df_bikes['Bike_type'] == bike_type].reset_index(drop=True)
#     ibd_forecast(one_bike_type, product_name=bike_type, value_string='OrderQuantity', path='Bikes')
#
# attention_bikes = ['Cosmo 2.0 T', 'Low rider 2.0']



# #good bikes
bike_types = df_bikes['Bike_type'].unique()
special_bikes = ['Blyss HS (Sample)', 'Blyss LS (Sample)', 'Bliss', 'Edge', 'Wave', 'Cosmo 2.0 T', 'Low rider 2.0']
good_bike_types = [item for item in bike_types if item not in special_bikes]

for bike_type in good_bike_types:
    one_bike_type = df_bikes.loc[df_bikes['Bike_type'] == bike_type].reset_index(drop=True)
    forecast(one_bike_type, product_name=bike_type, value_string='OrderQuantity', retrain=RETRAIN, path='Bikes')



#ACCESSORIES
path = 'Accessories'
quantity_value_string = 'OrderQuantity'
other_product_name = 'Other'

#top 5 accessory forecasts
top_5_accessories = df_accessories_top_5['ProductDescription'].unique()
for accessory in top_5_accessories:
    df_one_accessory = df_accessories_top_5[df_accessories_top_5['ProductDescription'] == accessory]
    forecast(df_one_accessory, product_name=accessory, value_string=quantity_value_string, retrain=RETRAIN, path=path)

#accessories other forecast
forecast(df_accessories_other, product_name=other_product_name, value_string=quantity_value_string, retrain=RETRAIN, path=path)

