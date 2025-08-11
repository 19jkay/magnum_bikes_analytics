import calendar
from datetime import datetime, date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def get_date_info():
    # Get today's date and format as string
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')

    ttm_start = today - relativedelta(years=1) + timedelta(days=1)
    ttm_start_str = ttm_start.strftime('%Y-%m-%d')

    return today_str, ttm_start_str