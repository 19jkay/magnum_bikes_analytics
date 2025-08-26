import re
from datetime import datetime

def convert_ms_date(ms_date_str):
    match = re.search(r'\d+', str(ms_date_str))
    if match:
        timestamp_ms = int(match.group())
        return datetime.utcfromtimestamp(timestamp_ms / 1000)
    return None  # Handle cases like NaN or malformed strings

