from Reports.Reports_Clean import Unleashed_PowerBI_SalesOrder_data, Unleashed_PowerBI_Inventory_data
from Reports.Reports_Helper import get_date_info
from Reports.Reports_Clean import Quickbooks_PowerBI_PandL_data


def PowerBI_data(reload):
    today_str, ttm_start_str = get_date_info()
    print("Today:", today_str)
    print("TTM Date:", ttm_start_str)

    df_TTM_SalesOrders = Unleashed_PowerBI_SalesOrder_data(start_date=ttm_start_str, end_date=today_str, reload=reload, save_excel=False)
    df_StockOnHand = Unleashed_PowerBI_Inventory_data(today_str, reload)
    df_PandL = Quickbooks_PowerBI_PandL_data(start_date=ttm_start_str, end_date=today_str, reload=reload)
    return df_TTM_SalesOrders, df_StockOnHand, df_PandL

#get TTM sales data for PowerBI
reload = True
df_TTM_SalesOrders, df_StockOnHand, df_PandL = PowerBI_data(reload=reload)