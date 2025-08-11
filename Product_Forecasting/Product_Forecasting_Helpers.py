import calendar
from datetime import datetime, date

def get_date_info():
    # Get today's date and format as string
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')

    # Get last day of previous month
    year = today.year
    month = today.month

    # Handle January (roll back to December of previous year)
    if month == 1:
        prev_month = 12
        year -= 1
    else:
        prev_month = month - 1

    # Get last day of the previous month
    last_day = calendar.monthrange(year, prev_month)[1]
    last_day_prev_month = date(year, prev_month, last_day)
    last_day_prev_month_str = last_day_prev_month.strftime('%Y-%m-%d')

    return today_str, last_day_prev_month_str