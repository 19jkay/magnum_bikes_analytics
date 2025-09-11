from Reports.Reports_Clean import Unleashed_PowerBI_SalesOrder_data, Unleashed_PowerBI_Inventory_data
from Reports.Reports_Helper import get_date_info
from Reports.Reports_Clean import Quickbooks_PowerBI_PandL_data, Unleashed_PowerBI_PurchaseOrders_data, Unleashed_PowerBI_WOH_report


def PowerBI_KPIs_data(reload):
    today_str, ttm_start_str = get_date_info()

    start_date = '2023-01-01'
    print("Today:", today_str)
    print("Start Date:", start_date)

    df_SalesOrders, df_SalesOrders_Bikes = Unleashed_PowerBI_SalesOrder_data(start_date=start_date, end_date=today_str, reload=reload, save_excel=False)
    # df_StockOnHand = Unleashed_PowerBI_Inventory_data(today_str, reload)
    # df_PurchaseOrders = Unleashed_PowerBI_PurchaseOrders_data(start_date=start_date, end_date=today_str, reload=reload, save_excel=False)
    # df_WOH = Unleashed_PowerBI_WOH_report(reload=reload)
    # df_PandL = Quickbooks_PowerBI_PandL_data(start_date=ttm_start_str, end_date=today_str, reload=reload)
    return df_SalesOrders



def PowerBI_Inventory_data(reload):
    df_WOH = Unleashed_PowerBI_WOH_report(reload=reload)
    return df_WOH


#get TTM sales data for PowerBI
reload = True
df_SalesOrders = PowerBI_KPIs_data(reload=reload)
df_WOH = PowerBI_Inventory_data(reload=reload)