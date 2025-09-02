from Unleashed_Data.Unleashed_Clean_Parallel import Unleashed_StockOnHand_clean_data_parallel
from Product_Forecasting.Product_Forecasting_Helpers import get_date_info

def DASH_Helper_get_product_info(product_name):
    today_str, last_day_prev_month_str = get_date_info()

    df_stockonhand = Unleashed_StockOnHand_clean_data_parallel(end_date=today_str, reload=False, save_excel=False)

    if product_name not in df_stockonhand['ProductDescription'].values:
        print("Product not in current data. Reloading... (This may take a minute)")
        df_stockonhand = Unleashed_StockOnHand_clean_data_parallel(end_date=today_str, reload=False, save_excel=False)

    df_stockonhand_product = df_stockonhand.loc[df_stockonhand['ProductDescription'] == product_name]
    product_guid = df_stockonhand_product['ProductGuid']
    avg_cost = df_stockonhand_product['AvgCost']

    return product_guid.iloc[0], avg_cost.iloc[0]



