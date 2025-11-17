from datetime import datetime, timedelta
import pandas as pd
from Unleashed_Data.Unleashed_Load_Parralelize import get_data_parallel
from AI_Automation.Dashboards.Operations_Demand_Scorecard.Operations_Demand_Scorecard_helper import unwrap_sales_orders, unwrap_warehouse_sales_orders, get_parts_list


#custom statuses are grouped into parked

def first_try_dashboard():
    print("First Try Dashboard")

    today = datetime.today()
    yesterday = today - timedelta(days=1)
    one_week_ago = today - timedelta(days=7)
    far_back = datetime(2025, 1, 1)

    #get open orders from (one year and week ago) to (week ago) this finds all open orders we had before reporting week
    start_date_far_back = far_back.strftime('%Y-%m-%d')
    end_date_one_week_ago = one_week_ago.strftime('%Y-%m-%d')
    df_salesOrders_before_week = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date_far_back,  end_date=end_date_one_week_ago)
    print(start_date_far_back)
    print(end_date_one_week_ago)

    #get old open orders
    old_status_counts = df_salesOrders_before_week['OrderStatus'].value_counts()
    old_parked = old_status_counts.get('Parked', 0)
    old_backordered = old_status_counts.get('Backordered', 0)
    old_placed = old_status_counts.get('Placed', 0)
    old_open_orders = old_placed + old_parked + old_backordered



    #get orders during this week so we can analyze orders we got this week
    start_date_one_week_ago = one_week_ago.strftime('%Y-%m-%d')
    end_date_today = today.strftime('%Y-%m-%d') #end date gives data up to yesterday since end date is not inclusive
    df_salesOrders_week = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date_one_week_ago,  end_date=end_date_today)
    print(start_date_one_week_ago)
    print(end_date_today)

    #Order status counts
    new_num_orders = len(df_salesOrders_week)
    status_counts = df_salesOrders_week['OrderStatus'].value_counts()
    this_week_completed = status_counts.get('Completed', 0)
    new_deleted = status_counts.get('Deleted', 0)
    new_parked = status_counts.get('Parked', 0)
    new_backordered = status_counts.get('Backordered', 0)
    new_placed = status_counts.get('Placed', 0)
    new_open_orders = new_placed + new_parked + new_backordered



    ops_summary_this_week = pd.DataFrame({
        "Metric": [
            "Old Open Orders",
            "New Orders This Week",
            "TOTAL ORDERS",
            "New Orders This Week Completed",
            "New Orders This Week Deleted",
            "TOTAL OPEN ORDERS"
        ],
        "Value": [
            old_open_orders,
            new_num_orders,
            old_open_orders + new_num_orders,
            this_week_completed,
            new_deleted,
            old_open_orders + new_open_orders
        ]
    })
    print(ops_summary_this_week)



    print("///////////////////////")
    ops_summary_open_orders = pd.DataFrame({
        "Metric": [
            "Old Open Orders Parked",
            "Old Open Orders Backordered",
            "Old Open Orders Placed",
            "TOTAL OLD OPEN ORDERS",
            "New Open Orders Parked",
            "New Open Orders Backordered",
            "New Open Orders Placed",
            "TOTAL NEW OPEN ORDERS",
            "TOTAL OPEN ORDERS"
        ],
        "Value": [
            old_parked,
            old_backordered,
            old_placed,
            old_open_orders,
            new_parked,
            new_backordered,
            new_placed,
            new_open_orders,
            old_open_orders + new_open_orders
        ]
    })
    print(ops_summary_open_orders)




    print("///////////////////////")
    df_SalesOrders_completed_dates = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date_one_week_ago, end_date=end_date_today)  # Sales Orders By Completed Date
    total_completed_orders_this_week = df_SalesOrders_completed_dates['OrderNumber'].nunique()
    ops_summary_completed = pd.DataFrame({
        "Metric": [
            "Old Orders Completed",
            "New Orders Completed",
            "TOTAL COMPLETED ORDERS"
        ],
        "Value": [
            total_completed_orders_this_week - this_week_completed,
            this_week_completed,
            total_completed_orders_this_week,
        ]
    })
    print(ops_summary_completed)






    #Now start looking at
    #completed bike orders = Completed orders - new orders completed

    #of all the completed orders, count the bike orders
    print("///////////////////////")
    df_unwrapped_salesorders_completed_date = unwrap_warehouse_sales_orders(df_SalesOrders_completed_dates)
    df_unwrapped_salesorders_JamN_bikes_completed_dates = df_unwrapped_salesorders_completed_date.loc[
        (df_unwrapped_salesorders_completed_date['WarehouseName'] == 'Jam-N Logistics') &
        (df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Bikes')] #DO I NEED TO EXCLUDE CUSTOMER TYPE = COSTCO? That is what makes the orderquantity sum large
    Total_num_Bike_Orders_JamN = df_unwrapped_salesorders_JamN_bikes_completed_dates['OrderNumber'].nunique()
    num_bikes_fulfilled_JamN = df_unwrapped_salesorders_JamN_bikes_completed_dates['OrderQuantity'].sum()

    ops_bike_summary_completed = pd.DataFrame({
        "Metric": [
            "Total # of Bike Orders: Jam-N",
            "# of Bikes Fulfilled: Jam-N",
            "Jam-N Bikes Per Order"
        ],
        "Value": [
            Total_num_Bike_Orders_JamN,
            num_bikes_fulfilled_JamN,
            num_bikes_fulfilled_JamN / Total_num_Bike_Orders_JamN
        ]
    })
    print(ops_bike_summary_completed)





    #Accessories
    print("///////////////////////")
    df_unwrapped_salesorders_accessories_order_dates = df_unwrapped_salesorders_completed_date.loc[df_unwrapped_salesorders_completed_date['ProductGroup'] == 'Accessories']
    total_accessories_orders = df_unwrapped_salesorders_accessories_order_dates['OrderNumber'].nunique()
    accessory_units_fulfilled = df_unwrapped_salesorders_accessories_order_dates['OrderQuantity'].sum()

    ops_accessory_summary_completed = pd.DataFrame({
        "Metric": [
            "Total Accessories Orders",
            "Accessory Units Fulfilled",
            "Accessories Per Completed Order"
        ],
        "Value": [
            total_accessories_orders,
            accessory_units_fulfilled,
            accessory_units_fulfilled / total_accessories_orders
        ]
    })
    print(ops_accessory_summary_completed)







    #Parts
    print("///////////////////////")
    df_unwrapped_salesorders_parts_completed_dates = df_unwrapped_salesorders_completed_date.loc[df_unwrapped_salesorders_completed_date['ProductGroup'].isin(get_parts_list())]
    total_parts_orders = df_unwrapped_salesorders_parts_completed_dates['OrderNumber'].nunique()
    print("Total Parts Orders: ", total_parts_orders)
    parts_units_fulfilled = df_unwrapped_salesorders_parts_completed_dates['OrderQuantity'].sum()
    print("Parts Units Fulfilled: ", parts_units_fulfilled)

    ops_parts_summary_completed = pd.DataFrame({
        "Metric": [
            "Total Parts Orders",
            "Parts Units Fulfilled",
            "Parts per Order"
        ],
        "Value": [
            total_parts_orders,
            parts_units_fulfilled,
            parts_units_fulfilled / total_parts_orders
        ]
    })
    print(ops_parts_summary_completed)


    #MAKE EXCEL FILE
    from openpyxl import load_workbook
    from openpyxl.styles import Font

    # --- Your existing summaries (already defined earlier in your script) ---
    # ops_summary_this_week
    # ops_summary_open_orders
    # ops_summary_completed
    # ops_bike_summary_completed
    # ops_accessory_summary_completed
    # ops_parts_summary_completed

    # Collect all summaries with titles
    sections = [
        ("This Week Summary", ops_summary_this_week),
        ("Open Orders Summary", ops_summary_open_orders),
        ("Completed Orders Summary", ops_summary_completed),
        ("Bike Orders Summary", ops_bike_summary_completed),
        ("Accessories Summary", ops_accessory_summary_completed),
        ("Parts Summary", ops_parts_summary_completed),
    ]

    # Build a combined DataFrame with titles + spacing
    combined = []
    for title, df in sections:
        # Title row
        combined.append(pd.DataFrame({"Metric": [title], "Value": [""]}))
        # Actual section
        combined.append(df)
        # Blank spacer row
        combined.append(pd.DataFrame({"Metric": [""], "Value": [""]}))

    final_df = pd.concat(combined, ignore_index=True)

    # Save to Excel at the requested path
    file_path = fr"C:\Users\joshu\Documents\Ops_Dashboard_Draft.xlsx"
    final_df.to_excel(file_path, index=False)

    # Optional: format titles bold
    wb = load_workbook(file_path)
    ws = wb.active
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, max_col=1):  # only "Metric" column
        cell = row[0]
        if cell.value in [title for title, _ in sections]:
            cell.font = Font(bold=True)
    wb.save(file_path)


def second_try_dashboard():
    print("Second Try Dashboard")
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    one_week_ago = today - timedelta(days=7)
    far_back = datetime(2025, 1, 1)

    # get open orders from (one year and week ago) to (week ago) this finds all open orders we had before reporting week
    start_date_far_back = far_back.strftime('%Y-%m-%d')
    end_date_one_week_ago = one_week_ago.strftime('%Y-%m-%d')
    df_salesOrders_before_week = get_data_parallel(unleashed_data_name="SalesOrdersDate",
                                                   start_date=start_date_far_back, end_date=end_date_one_week_ago)
    print(start_date_far_back)
    print(end_date_one_week_ago)

    # get old open orders
    old_status_counts = df_salesOrders_before_week['OrderStatus'].value_counts()
    old_parked = old_status_counts.get('Parked', 0)
    old_backordered = old_status_counts.get('Backordered', 0)
    old_placed = old_status_counts.get('Placed', 0)
    old_open_orders = old_placed + old_parked + old_backordered

    # get orders during this week so we can analyze orders we got this week
    start_date_one_week_ago = one_week_ago.strftime('%Y-%m-%d')
    end_date_today = today.strftime('%Y-%m-%d')  # end date gives data up to yesterday since end date is not inclusive
    df_salesOrders_week = get_data_parallel(unleashed_data_name="SalesOrdersDate", start_date=start_date_one_week_ago,
                                            end_date=end_date_today)
    print(start_date_one_week_ago)
    print(end_date_today)

    # Order status counts
    new_num_orders = len(df_salesOrders_week)
    status_counts = df_salesOrders_week['OrderStatus'].value_counts()
    this_week_completed = status_counts.get('Completed', 0)
    new_deleted = status_counts.get('Deleted', 0)
    new_parked = status_counts.get('Parked', 0)
    new_backordered = status_counts.get('Backordered', 0)
    new_placed = status_counts.get('Placed', 0)
    new_open_orders = new_placed + new_parked + new_backordered

    df_SalesOrders_completed_dates = get_data_parallel(unleashed_data_name="SalesOrders", start_date=start_date_one_week_ago, end_date=end_date_today)  # Sales Orders By Completed Date
    total_completed_orders_this_week = df_SalesOrders_completed_dates['OrderNumber'].nunique()

    ops_summary_productivity_WTD = pd.DataFrame({
        "Metric": [
            "New Orders",
            "Completed Orders",
            "*Deleted (DELETED ORDERS OF CREATED WTD)",
            "*Completed % (USES DELETED)"
        ],
        "Value": [
            new_num_orders,
            total_completed_orders_this_week,
            new_deleted,
            (total_completed_orders_this_week + new_deleted) / new_num_orders
        ]
    })
    print(ops_summary_productivity_WTD)

    ops_summary_work_in_progres_ytd = pd.DataFrame({
        "Metric": [
            "Parked",
            "Backordered",
            "Hold + Accounting(Sales)",
            "Placed(Bikes) or Ready to Ship(parts & accessories)",
            "Total Open Orders",
            "# of Units associated with Pre-Orders"
        ],
        "Value": [
            old_parked + new_parked,
            old_backordered + new_backordered,
            new_deleted,
            (total_completed_orders_this_week + new_deleted) / new_num_orders
        ]
    })
    print(ops_summary_work_in_progres_ytd)