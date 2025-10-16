import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import json
import hmac
import hashlib
import base64

import pandas as pd

#PEIYA

DF20250620_empty = []

DF20250620_data = [
    # PO 870 group
    {"Invoice No": "DF20250620", "SKU": "23110059",      "Quantity": 2,   "Amount": 65.60,   "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "23110071",      "Quantity": 2,   "Amount": 52.80,   "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "ELCO1044DISP8015","Quantity": 38,"Amount": 912.00,  "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "ELCO1044CONT0022","Quantity": 22,"Amount": 770.00,  "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "ELCO1044CONT8030","Quantity": 20,"Amount": 700.00,  "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "23110210",      "Quantity": 60,  "Amount": 36.00,   "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "2213170024",    "Quantity": 5,   "Amount": 152.50,  "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "23110191",      "Quantity": 10,  "Amount": 320.00,  "PO NO": "870"},
    {"Invoice No": "DF20250620", "SKU": "23110077",      "Quantity": 5,   "Amount": 87.50,   "PO NO": "870"},

    # PO 873 group
    {"Invoice No": "DF20250620", "SKU": "25110017",      "Quantity": 20,  "Amount": 536.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110018",      "Quantity": 20,  "Amount": 680.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110019",      "Quantity": 10,  "Amount": 340.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "ELCO1010CHAR0076","Quantity": 50,"Amount": 625.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110014",      "Quantity": 20,  "Amount": 460.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110016",      "Quantity": 20,  "Amount": 460.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110015",      "Quantity": 10,  "Amount": 280.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110012",      "Quantity": 20,  "Amount": 320.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110043",      "Quantity": 19,  "Amount": 551.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110044",      "Quantity": 55,  "Amount": 104.50,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110048",      "Quantity": 15,  "Amount": 51.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110049",      "Quantity": 20,  "Amount": 100.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110050",      "Quantity": 10,  "Amount": 50.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110051",      "Quantity": 20,  "Amount": 50.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110055",      "Quantity": 20,  "Amount": 156.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110057",      "Quantity": 20,  "Amount": 132.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110058",      "Quantity": 20,  "Amount": 110.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "23110001",      "Quantity": 5,   "Amount": 49.50,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110059",      "Quantity": 10,  "Amount": 140.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "23110002",      "Quantity": 5,   "Amount": 90.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110069",      "Quantity": 10,  "Amount": 5.00,    "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110070",      "Quantity": 10,  "Amount": 5.00,    "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110071",      "Quantity": 5,   "Amount": 7.50,    "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "23110002",      "Quantity": 10,  "Amount": 60.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110072",      "Quantity": 15,  "Amount": 55.50,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110079",      "Quantity": 55,  "Amount": 137.50,  "PO NO": "873"},    #thing
    {"Invoice No": "DF20250620", "SKU": "25110076",      "Quantity": 55,  "Amount": 236.50,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110077",      "Quantity": 55,  "Amount": 275.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110036",      "Quantity": 15,  "Amount": 75.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110037",      "Quantity": 15,  "Amount": 84.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110038",      "Quantity": 15,  "Amount": 60.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110039",      "Quantity": 15,  "Amount": 99.00,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110078",      "Quantity": 55,  "Amount": 616.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110060",      "Quantity": 5,   "Amount": 77.50,   "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110061",      "Quantity": 5,   "Amount": 110.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110062",      "Quantity": 5,   "Amount": 113.00,  "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110028",      "Quantity": 20,  "Amount": 1240.00, "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110029",      "Quantity": 20,  "Amount": 1360.00, "PO NO": "873"},
    {"Invoice No": "DF20250620", "SKU": "25110030",      "Quantity": 10,  "Amount": 750.00,  "PO NO": "873"},

    # PO 853 group
    {"Invoice No": "DF20250620", "SKU": "23110095",      "Quantity": 10,  "Amount": 6.50,    "PO NO": "853"},
    {"Invoice No": "DF20250620", "SKU": "23110108",      "Quantity": 10,  "Amount": 190.00,  "PO NO": "853"},
    {"Invoice No": "DF20250620", "SKU": "ELCO1044DISP0010","Quantity": 100,"Amount": 2400.00,"PO NO": "853"},

    # Blank PO (empty string where PO NO not provided in the table)
    {"Invoice No": "DF20250620", "SKU": "25110052",      "Quantity": 5,   "Amount": 175.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110053",      "Quantity": 5,   "Amount": 165.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110054",      "Quantity": 5,   "Amount": 182.50,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110017",      "Quantity": 28,  "Amount": 750.40,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110018",      "Quantity": 30,  "Amount": 1020.00, "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110019",      "Quantity": 20,  "Amount": 680.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "ELCO1010CHAR0076","Quantity": 30,"Amount": 375.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110014",      "Quantity": 28,  "Amount": 644.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110016",      "Quantity": 28,  "Amount": 644.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110015",      "Quantity": 18,  "Amount": 504.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110012",      "Quantity": 20,  "Amount": 320.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110031",      "Quantity": 20,  "Amount": 406.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110032",      "Quantity": 40,  "Amount": 948.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110033",      "Quantity": 30,  "Amount": 711.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110034",      "Quantity": 25,  "Amount": 525.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110035",      "Quantity": 25,  "Amount": 582.50,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110040",      "Quantity": 50,  "Amount": 125.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110041",      "Quantity": 29,  "Amount": 89.90,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "2215050030",    "Quantity": 49,  "Amount": 161.70,  "PO NO": "875"},

    # PO 875 group (page 2)
    {"Invoice No": "DF20250620", "SKU": "25110048",      "Quantity": 14,  "Amount": 47.60,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110066",      "Quantity": 10,  "Amount": 56.00,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110067",      "Quantity": 10,  "Amount": 48.50,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110068",      "Quantity": 5,   "Amount": 44.00,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110073",      "Quantity": 10,  "Amount": 350.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110074",      "Quantity": 15,  "Amount": 615.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110075",      "Quantity": 10,  "Amount": 410.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "23110001",      "Quantity": 5,   "Amount": 49.50,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110059",      "Quantity": 20,  "Amount": 280.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "23110002",      "Quantity": 4,   "Amount": 72.00,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "DERA1010HANG1000","Quantity": 10,"Amount": 10.00,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "FRAM1052HANG2425","Quantity": 10,"Amount": 10.00,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "2213170022",    "Quantity": 5,   "Amount": 5.00,    "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110049",      "Quantity": 20,  "Amount": 100.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110050",      "Quantity": 15,  "Amount": 75.00,   "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110069",      "Quantity": 10,  "Amount": 5.00,    "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110071",      "Quantity": 5,   "Amount": 7.50,    "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110062",      "Quantity": 5,   "Amount": 113.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110028",      "Quantity": 11,  "Amount": 682.00,  "PO NO": "875"},
    {"Invoice No": "DF20250620", "SKU": "25110030",      "Quantity": 14,  "Amount": 1050.00, "PO NO": "875"},

    # PO 877 group
    {"Invoice No": "DF20250620", "SKU": "23110212",      "Quantity": 250, "Amount": 2375.00, "PO NO": "877"},
    {"Invoice No": "DF20250620", "SKU": "24110026",      "Quantity": 100, "Amount": 950.00,  "PO NO": "877"},
]

DF20250624_empty = []


DF20250624_O_data = [
    {"Invoice No": "DF20250624-O", "SKU": "ELCO1010CONT0118", "Quantity": 15,  "Amount": 525.00,  "PO NO": "853"},
    {"Invoice No": "DF20250624-O", "SKU": "25110043",         "Quantity": 1,   "Amount": 29.00,   "PO NO": "873"},
    {"Invoice No": "DF20250624-O", "SKU": "25110065",         "Quantity": 5,   "Amount": 24.50,   "PO NO": "873"},
    {"Invoice No": "DF20250624-O", "SKU": "25110045",         "Quantity": 20,  "Amount": 180.00,  "PO NO": "873"},
    {"Invoice No": "DF20250624-O", "SKU": "23110002",         "Quantity": 20,  "Amount": 120.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110045",         "Quantity": 20,  "Amount": 180.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110046",         "Quantity": 20,  "Amount": 276.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110047",         "Quantity": 20,  "Amount": 280.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110042",         "Quantity": 20,  "Amount": 560.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110013",         "Quantity": 20,  "Amount": 72.00,   "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110056",         "Quantity": 5,   "Amount": 26.50,   "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110064",         "Quantity": 10,  "Amount": 49.00,   "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110065",         "Quantity": 5,   "Amount": 24.50,   "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110078",         "Quantity": 55,  "Amount": 616.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110060",         "Quantity": 5,   "Amount": 77.50,   "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110061",         "Quantity": 5,   "Amount": 110.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110028",         "Quantity": 9,   "Amount": 558.00,  "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110029",         "Quantity": 20,  "Amount": 1360.00, "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "25110030",         "Quantity": 1,   "Amount": 75.00,   "PO NO": "875"},
    {"Invoice No": "DF20250624-O", "SKU": "23110176",         "Quantity": 300, "Amount": 300.00,  "PO NO": "876"},
]

#no purchase order but expensive
data_PY20250920 = []

PY20250826_O_data = [
    {"Invoice No": "PY20250826-O", "SKU": "25110044",           "Quantity": 55,  "Amount": 104.5,   "PO NO": "875"},
    {"Invoice No": "PY20250826-O", "SKU": "25110091",           "Quantity": 40,  "Amount": 112.0,   "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "FRAM1044HDST1044",   "Quantity": 20,  "Amount": 80.0,    "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "2213170054",         "Quantity": 60,  "Amount": 2100.0,  "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "24110032",           "Quantity": 10,  "Amount": 25.0,    "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "2213170046",         "Quantity": 10,  "Amount": 0.0,     "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "PERF1044LIHT1050",   "Quantity": 5,   "Amount": 36.0,    "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "23110024",           "Quantity": 5,   "Amount": 28.25,   "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "23110100",           "Quantity": 20,  "Amount": 700.0,   "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "ELCO1044CONT0020",   "Quantity": 35,  "Amount": 1225.0,  "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "25110100",           "Quantity": 200, "Amount": 0.0,     "PO NO": "898"},
    {"Invoice No": "PY20250826-O", "SKU": "25110012",           "Quantity": 20,  "Amount": 320.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110051",           "Quantity": 10,  "Amount": 25.0,    "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110049",           "Quantity": 20,  "Amount": 100.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110055",           "Quantity": 10,  "Amount": 78.0,    "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110044",           "Quantity": 20,  "Amount": 38.0,    "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110064",           "Quantity": 5,   "Amount": 24.5,    "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110017",           "Quantity": 20,  "Amount": 536.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110014",           "Quantity": 20,  "Amount": 470.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110095",           "Quantity": 20,  "Amount": 434.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110018",           "Quantity": 20,  "Amount": 680.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110073",           "Quantity": 5,   "Amount": 175.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110074",           "Quantity": 5,   "Amount": 205.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "25110016",           "Quantity": 20,  "Amount": 470.0,   "PO NO": "884"},
    {"Invoice No": "PY20250826-O", "SKU": "23110083",           "Quantity": 5,   "Amount": 155.0,   "PO NO": "870"},
    {"Invoice No": "PY20250826-O", "SKU": "COPI1044BRAK6069",   "Quantity": 15,  "Amount": 300.0,   "PO NO": "870"},
    {"Invoice No": "PY20250826-O", "SKU": "COPI1044FHDR0008",   "Quantity": 15,  "Amount": 207.0,   "PO NO": "870"},
    {"Invoice No": "PY20250826-O", "SKU": "2213170030",         "Quantity": 3,   "Amount": 105.0,   "PO NO": "870"},
    # {"Invoice No": "PY20250826-O", "SKU": "25110079",           "Quantity": 90,  "Amount": 90.0,    "PO NO": "873&875"},     #thing
    {"Invoice No": "PY20250826-O", "SKU": "23110106",           "Quantity": 5,   "Amount": 397.5,   "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "23110158",           "Quantity": 10,  "Amount": 785.0,   "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "23110206",           "Quantity": 5,   "Amount": 392.5,   "PO NO": "880"},
    {"Invoice No": "PY20250826-O", "SKU": "23110061",           "Quantity": 20,  "Amount": 180.0,   "PO NO": "880"},
]

PY20250903_O_data = [
    {"Invoice No": "PY20250903-O", "SKU": "25110038",        "Quantity": 10,  "Amount": 40.0,    "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110039",        "Quantity": 10,  "Amount": 66.0,    "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110036",        "Quantity": 10,  "Amount": 50.0,    "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110037",        "Quantity": 10,  "Amount": 56.0,    "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110061",        "Quantity": 5,   "Amount": 110.0,   "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110028",        "Quantity": 5,   "Amount": 332.5,   "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110060",        "Quantity": 5,   "Amount": 77.5,    "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "25110029",        "Quantity": 5,   "Amount": 385.0,   "PO NO": "884"},
    {"Invoice No": "PY20250903-O", "SKU": "2215050009",      "Quantity": 20,  "Amount": 420.0,   "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "23110203",        "Quantity": 30,  "Amount": 1050.0,  "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "ELCO1044CONT8030", "Quantity": 40,  "Amount": 1400.0,  "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "2215050008",      "Quantity": 20,  "Amount": 400.0,   "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "ACCE1290KICK1011", "Quantity": 5,   "Amount": 16.0,    "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "ELCO1044THRT5006", "Quantity": 20,  "Amount": 76.0,    "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "2213170024",      "Quantity": 5,   "Amount": 152.5,   "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "23110016",        "Quantity": 10,  "Amount": 110.0,   "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "2215050003",      "Quantity": 5,   "Amount": 12.5,    "PO NO": "880"},
    {"Invoice No": "PY20250903-O", "SKU": "25110077",        "Quantity": 55,  "Amount": 275.0,   "PO NO": "875"},
    {"Invoice No": "PY20250903-O", "SKU": "25110076",        "Quantity": 55,  "Amount": 236.5,   "PO NO": "875"},
    {"Invoice No": "PY20250903-O", "SKU": "25110039",        "Quantity": 15,  "Amount": 99.0,    "PO NO": "875"},
    {"Invoice No": "PY20250903-O", "SKU": "25110037",        "Quantity": 15,  "Amount": 84.0,    "PO NO": "875"},
    {"Invoice No": "PY20250903-O", "SKU": "2213170017",      "Quantity": 10,  "Amount": 180.0,   "PO NO": "899"},
    {"Invoice No": "PY20250903-O", "SKU": "2213170019",      "Quantity": 5,   "Amount": 65.0,    "PO NO": "899"},
    {"Invoice No": "PY20250903-O", "SKU": "2215050031",      "Quantity": 10,  "Amount": 22.0,    "PO NO": "899"},
    {"Invoice No": "PY20250903-O", "SKU": "2213170021",      "Quantity": 10,  "Amount": 135.0,   "PO NO": "899"},
    {"Invoice No": "PY20250903-O", "SKU": "25110104",        "Quantity": 10,  "Amount": 40.0,    "PO NO": "896"},
    {"Invoice No": "PY20250903-O", "SKU": "FRAM1044HDST0012", "Quantity": 5,   "Amount": 18.0,    "PO NO": "890"},
    {"Invoice No": "PY20250903-O", "SKU": "2213170011",      "Quantity": 5,   "Amount": 5.1,     "PO NO": "890"},
    {"Invoice No": "PY20250903-O", "SKU": "2213170034",      "Quantity": 10,  "Amount": 47.0,    "PO NO": "890"},
    {"Invoice No": "PY20250903-O", "SKU": "2215050027",      "Quantity": 5,   "Amount": 175.0,   "PO NO": "890"},
    {"Invoice No": "PY20250903-O", "SKU": "PERF1010FLIG0047", "Quantity": 10,  "Amount": 28.0,    "PO NO": "890"},
    {"Invoice No": "PY20250903-O", "SKU": "RWAS4203MOTO0101", "Quantity": 25,  "Amount": 2040.0,  "PO NO": "750"},
]

PY20250905_O_data = [
    {"Invoice No": "PY20250905-O", "SKU": "ELCO1037CONT0006", "Quantity": 95,  "Amount": 4085.0,  "PO NO": "870"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO3050CONT0058", "Quantity": 100, "Amount": 3700.0,  "PO NO": "870"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO1044DISP8015", "Quantity": 5,   "Amount": 120.0,   "PO NO": "890"},
    {"Invoice No": "PY20250905-O", "SKU": "2213170067",       "Quantity": 5,   "Amount": 392.5,   "PO NO": "890"},
    {"Invoice No": "PY20250905-O", "SKU": "COPI1010GRIP0092", "Quantity": 5,   "Amount": 14.5,    "PO NO": "890"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO1044MOTO0012", "Quantity": 12,  "Amount": 0.0,     "PO NO": "822"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO1044MOTO0028", "Quantity": 1,   "Amount": 0.0,     "PO NO": "828"},
    {"Invoice No": "PY20250905-O", "SKU": "25110052",         "Quantity": 4,   "Amount": 140.0,   "PO NO": "884"},
    {"Invoice No": "PY20250905-O", "SKU": "25110097",         "Quantity": 5,   "Amount": 182.5,   "PO NO": "884"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO1048DISP8001", "Quantity": 50,  "Amount": 1200.0,  "PO NO": "896"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO0000DISP1493", "Quantity": 20,  "Amount": 260.0,   "PO NO": "899"},
    {"Invoice No": "PY20250905-O", "SKU": "ELCO1048DISP0004", "Quantity": 15,  "Amount": 460.5,   "PO NO": "890"},
]

#NO PO NO
PY2025920_O_data = [
    {"Invoice No": "PY2025920-O", "SKU": "25150007",                "Quantity": 652.5, "Amount": 6525.0,   "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150006",                "Quantity": 649.5, "Amount": 24681.0,  "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150008",                "Quantity": 652.5, "Amount": 13050.0,  "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150016",                "Quantity": 659.5, "Amount": 6595.0,   "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150009",                "Quantity": 667.0, "Amount": 5336.0,   "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150012",                "Quantity": 674.0, "Amount": 22916.0,  "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150011",                "Quantity": 667.0, "Amount": 22678.0,  "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "25150010",                "Quantity": 677.0, "Amount": 1354.0,   "PO NO": "897"},
    {"Invoice No": "PY2025920-O", "SKU": "Folding Bike-Sample - Red","Quantity": 660.0, "Amount": 660.0,    "PO NO": ""},
    {"Invoice No": "PY2025920-O", "SKU": "Folding Bike-Sample - Ocean","Quantity": 661.0,"Amount": 661.0,    "PO NO": ""},
    {"Invoice No": "PY2025920-O", "SKU": "City Full suspension sample","Quantity": 662.0,"Amount": 662.0,    "PO NO": ""},
    {"Invoice No": "PY2025920-O", "SKU": "23110191",                "Quantity": 10.0,  "Amount": 320.0,    "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "CPOI1044RHDR0010",        "Quantity": 10.0,  "Amount": 138.0,    "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "221317005",               "Quantity": 10.0,  "Amount": 82.0,     "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "2215050027",              "Quantity": 10.0,  "Amount": 350.0,    "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "2215050036",              "Quantity": 30.0,  "Amount": 390.0,    "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "25110103",                "Quantity": 10.0,  "Amount": 38.0,     "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "ELCO0000CONT1491",        "Quantity": 15.0,  "Amount": 270.0,    "PO NO": "896"},
    {"Invoice No": "PY2025920-O", "SKU": "ELCO1048DISP0004",        "Quantity": 15.0,  "Amount": 490.5,    "PO NO": "890"},
    {"Invoice No": "PY2025920-O", "SKU": "DRCH1200PEDA0004",        "Quantity": 40.0,  "Amount": 152.0,    "PO NO": "902"},
    {"Invoice No": "PY2025920-O", "SKU": "25110052",                "Quantity": 1.0,   "Amount": 35.0,     "PO NO": "884"},
    {"Invoice No": "PY2025920-O", "SKU": "ELCO1010TREE0025",        "Quantity": 200.0, "Amount": 640.0,    "PO NO": "899"},
    {"Invoice No": "PY2025920-O", "SKU": "24110018",                "Quantity": 3.0,   "Amount": 45.0,     "PO NO": "899"},
    {"Invoice No": "PY2025920-O", "SKU": "ELCO1048DISP0004",        "Quantity": 15.0,  "Amount": 490.5,    "PO NO": "899"},
    {"Invoice No": "PY2025920-O", "SKU": "2213170046",              "Quantity": 5.0,   "Amount": 9.0,      "PO NO": "899"},
    {"Invoice No": "PY2025920-O", "SKU": "23110062",                "Quantity": 20.0,  "Amount": 182.0,    "PO NO": "880"},
    {"Invoice No": "PY2025920-O", "SKU": "FRAM1282CARR0004",        "Quantity": 25.0,  "Amount": 237.5,    "PO NO": "889"},
    {"Invoice No": "PY2025920-O", "SKU": "Frame tubes For Edge",    "Quantity": 10.0,  "Amount": 700.0,    "PO NO": ""}
]

PY2025929_O_data = [
    {"Invoice No": "PY2025929-O", "SKU": "ELCO1048DISP0004", "Quantity": 15, "Amount": 490.5, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "23110017", "Quantity": 20, "Amount": 480.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "2213170030", "Quantity": 10, "Amount": 350.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "2215050025", "Quantity": 30, "Amount": 720.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "22110205", "Quantity": 30, "Amount": 720.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "2213170047", "Quantity": 10, "Amount": 240.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "2213170055", "Quantity": 5, "Amount": 40.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "23110012", "Quantity": 5, "Amount": 65.0, "PO NO": "PO899"},
    {"Invoice No": "PY2025929-O", "SKU": "ELCO1010SEAT00E4", "Quantity": 10, "Amount": 35.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "ELCO1010SEAT00E3", "Quantity": 10, "Amount": 63.0, "PO NO": "899"},
    {"Invoice No": "PY2025929-O", "SKU": "24110020", "Quantity": 10, "Amount": 350.0, "PO NO": "904"},
    {"Invoice No": "PY2025929-O", "SKU": "2213170057", "Quantity": 2, "Amount": 14.0, "PO NO": "896"},
    {"Invoice No": "PY2025929-O", "SKU": "2213170024", "Quantity": 5, "Amount": 165.0, "PO NO": "896"},
    {"Invoice No": "PY2025929-O", "SKU": "23110077", "Quantity": 5, "Amount": 96.5, "PO NO": "896"},
    {"Invoice No": "PY2025929-O", "SKU": "25110040", "Quantity": 20, "Amount": 50.0, "PO NO": "884"},
    {"Invoice No": "PY2025929-O", "SKU": "RTAS1402SPOK0071", "Quantity": 100, "Amount": 9.0, "PO NO": "880"},
    {"Invoice No": "PY2025929-O", "SKU": "RTAS1402SPOK0067", "Quantity": 120, "Amount": 10.8, "PO NO": "880"},
    {"Invoice No": "PY2025929-O", "SKU": "23110190", "Quantity": 400, "Amount": 1000.0, "PO NO": "909"},
    {"Invoice No": "PY2025929-O", "SKU": "FRAM1044SEPO0019", "Quantity": 5, "Amount": 17.0, "PO NO": "902"},
    {"Invoice No": "PY2025929-O", "SKU": "WHAS1160TIRE25FT", "Quantity": 10, "Amount": 190.0, "PO NO": "902"},
    {"Invoice No": "PY2025929-O", "SKU": "WHAS1160TIRE21FT", "Quantity": 10, "Amount": 160.0, "PO NO": "902"},
]

PY20250806_O_data = [
    {"Invoice No": "PY20250806-O", "SKU": "25110043", "Quantity": 20, "Amount": 560.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "25110051", "Quantity": 20, "Amount": 50.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1010CHAR0076", "Quantity": 20, "Amount": 240.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "25110079", "Quantity": 55, "Amount": 137.5, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "23110002", "Quantity": 1, "Amount": 18.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "25110070", "Quantity": 10, "Amount": 5.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "25110038", "Quantity": 15, "Amount": 60.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "25110036", "Quantity": 15, "Amount": 75.0, "PO NO": "875"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1010CHAR0076", "Quantity": 50, "Amount": 600.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "23110193", "Quantity": 20, "Amount": 20.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO3140BASE0022", "Quantity": 10, "Amount": 14.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "PERF1044FLIG0004", "Quantity": 15, "Amount": 60.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1044CABL1039", "Quantity": 10, "Amount": 53.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170013", "Quantity": 10, "Amount": 77.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2215050022", "Quantity": 10, "Amount": 74.6, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "ACCE1290KICK0004", "Quantity": 60, "Amount": 240.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170017", "Quantity": 5, "Amount": 90.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "DRTN1044CRNK0086", "Quantity": 10, "Amount": 128.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "23110194", "Quantity": 20, "Amount": 72.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1044DISP8015", "Quantity": 30, "Amount": 720.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2215050037", "Quantity": 10, "Amount": 35.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "24110019", "Quantity": 20, "Amount": 80.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170062", "Quantity": 50, "Amount": 375.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2215050048", "Quantity": 20, "Amount": 48.0, "PO NO": "PO880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1044CHAR0 006", "Quantity": 30, "Amount": 510.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "COPI1223SHFTF0000", "Quantity": 5, "Amount": 15.5, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "23110207", "Quantity": 20, "Amount": 76.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1010PASS0068", "Quantity": 20, "Amount": 66.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "23110171", "Quantity": 10, "Amount": 26.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "COPI1223SHFTF0002", "Quantity": 5, "Amount": 22.5, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170042", "Quantity": 10, "Amount": 33.0, "PO NO": "PO880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO3140BACO0043", "Quantity": 150, "Amount": 705.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "FRAM1044TOPS0000", "Quantity": 30, "Amount": 231.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "WHEL1044TUBE6002", "Quantity": 20, "Amount": 56.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170047", "Quantity": 30, "Amount": 720.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1010CONT0118", "Quantity": 20, "Amount": 660.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "DRTN1044CRNK0014", "Quantity": 15, "Amount": 165.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "23110003", "Quantity": 10, "Amount": 192.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170019", "Quantity": 3, "Amount": 39.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "23110084", "Quantity": 20, "Amount": 70.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170087", "Quantity": 50, "Amount": 40.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170012", "Quantity": 10, "Amount": 36.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2213170018", "Quantity": 6, "Amount": 294.0, "PO NO": "880"},
    {"Invoice No": "PY20250806-O", "SKU": "2215050036", "Quantity": 26, "Amount": 338.0, "PO NO": "890"},
    {"Invoice No": "PY20250806-O", "SKU": "25110058", "Quantity": 10, "Amount": 55.0, "PO NO": "884"},
    {"Invoice No": "PY20250806-O", "SKU": "25110032", "Quantity": 30, "Amount": 711.0, "PO NO": "884"},
    {"Invoice No": "PY20250806-O", "SKU": "23110203", "Quantity": 21, "Amount": 735.0, "PO NO": "870"},
    {"Invoice No": "PY20250806-O", "SKU": "ELCO1044MOTO0031", "Quantity": 25, "Amount": 1575.0, "PO NO": "871"},
    {"Invoice No": "PY20250806-O", "SKU": "25110005", "Quantity": 15, "Amount": 960.0, "PO NO": "871"},
]

data_20250721 = [
    {"Invoice No": "20250721", "SKU": "FTAS0001FRAM0001", "Quantity": 300, "Amount": 183.0, "PO NO": "880"},
    {"Invoice No": "20250721", "SKU": "23110203", "Quantity": 9, "Amount": 315.0, "PO NO": "870"},
    {"Invoice No": "20250721", "SKU": "", "Quantity": 1, "Amount": 170.0, "PO NO": ""},
]

data_20250822_F1 = []

data_20250825_1 = [
    {"Invoice No": "20250825-1", "SKU": "ELCO1044BATT0008 UL", "Quantity": 100, "Amount": 14300.0, "PO NO": "880"},
    {"Invoice No": "20250825-1", "SKU": "Freight", "Quantity": 1, "Amount": 1270.2, "PO NO": ""},
]

data_20250825_2 = [
    {"Invoice No": "20250825-2", "SKU": "", "Quantity": 1, "Amount": 0.0, "PO NO": ""},
    {"Invoice No": "20250825-2", "SKU": "2215050036", "Quantity": 4, "Amount": 52.0, "PO NO": "890"},
    {"Invoice No": "20250825-2", "SKU": "25110105", "Quantity": 40, "Amount": 0.0, "PO NO": "901"},
    {"Invoice No": "20250825-2", "SKU": "", "Quantity": 1, "Amount": 72.0, "PO NO": ""},
]

data_20250825_3 = [
    {"Invoice No": "20250825-3", "SKU": "ELCO1044DISP8015", "Quantity": 10, "Amount": 240.0, "PO NO": "891"},
    {"Invoice No": "20250825-3", "SKU": "ELCO1044CONT8030", "Quantity": 10, "Amount": 350.0, "PO NO": "891"},
    {"Invoice No": "20250825-3", "SKU": "ELCO1044CABL1039", "Quantity": 10, "Amount": 50.0, "PO NO": "900"},
    {"Invoice No": "20250825-3", "SKU": "", "Quantity": 1, "Amount": 106.0, "PO NO": ""},
]

data_20250825_4 = [
    {"Invoice No": "20250825-4", "SKU": "ELCO1037BATT14EB", "Quantity": 1, "Amount": 245.0, "PO NO": "870"},
    {"Invoice No": "20250825-4", "SKU": "", "Quantity": 1, "Amount": 110.5, "PO NO": ""},
]

data_20250825_F1 = []

data_20250830_F = []

data_20250912_1 = [
    {"Invoice No": "20250912-1", "SKU": "23110063", "Quantity": 10, "Amount": 2.0, "PO NO": "880"},
    {"Invoice No": "20250912-1", "SKU": "2213170030", "Quantity": 2, "Amount": 70.0, "PO NO": "870"},
    {"Invoice No": "20250912-1", "SKU": "ELCO1048DISP8001", "Quantity": 20, "Amount": 480.0, "PO NO": "900"},
    {"Invoice No": "20250912-1", "SKU": "", "Quantity": 1, "Amount": 15.0, "PO NO": ""},
    {"Invoice No": "20250912-1", "SKU": "", "Quantity": 1, "Amount": 0.0, "PO NO": ""},
    {"Invoice No": "20250912-1", "SKU": "", "Quantity": 1, "Amount": 7.0, "PO NO": ""},
    {"Invoice No": "20250912-1", "SKU": "", "Quantity": 20, "Amount": 20.0, "PO NO": "873"},
    {"Invoice No": "20250912-1", "SKU": "", "Quantity": 1, "Amount": 82.0, "PO NO": ""},
]

data_20250912_2 = [
    {"Invoice No": "20250912-2", "SKU": "ELCO1037CONT0006", "Quantity": 5, "Amount": 215.0, "PO NO": "870"},
    {"Invoice No": "20250912-2", "SKU": "ELCO1044DISP8015", "Quantity": 25, "Amount": 600.0, "PO NO": "890"},
    {"Invoice No": "20250912-2", "SKU": "", "Quantity": 1, "Amount": 86.0, "PO NO": ""},
]

data_DF20250620_F = []
data_DF20250624_F = []

data_PY20250822_1 = [
    {"Invoice No": "PY20250822-1", "SKU": "ELCO1044BATT0008 UL", "Quantity": 40, "Amount": 5720.0, "PO NO": "870"},
    {"Invoice No": "PY20250822-1", "SKU": "ELCO3150BATT00", "Quantity": 10, "Amount": 1750.0, "PO NO": "870"},
    {"Invoice No": "PY20250822-1", "SKU": "ELCOBI53BATT13", "Quantity": 25, "Amount": 2625.0, "PO NO": "870"},
    {"Invoice No": "PY20250822-1", "SKU": "23110165", "Quantity": 10, "Amount": 1300.0, "PO NO": "870"},
    {"Invoice No": "PY20250822-1", "SKU": "23110165", "Quantity": 5, "Amount": 650.0, "PO NO": "848"},
]

data_PY20250822_2 = [
    {"Invoice No": "PY20250822-2", "SKU": "25110020", "Quantity": 4, "Amount": 576.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110021", "Quantity": 4, "Amount": 576.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110022", "Quantity": 9, "Amount": 1296.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110023", "Quantity": 3, "Amount": 432.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110024", "Quantity": 5, "Amount": 720.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110025", "Quantity": 2, "Amount": 288.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110026", "Quantity": 5, "Amount": 720.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110027", "Quantity": 10, "Amount": 1770.0, "PO NO": "873"},
    {"Invoice No": "PY20250822-2", "SKU": "25110027", "Quantity": 50, "Amount": 8850.0, "PO NO": "875"},
    {"Invoice No": "PY20250822-2", "SKU": "ELCO1044BATT0009 UL", "Quantity": 60, "Amount": 10500.0, "PO NO": "870"},
]

data_PY20250930_O = [
    {"Invoice No": "PY20250930-O", "SKU": "25150014", "Quantity": 92, "Amount": 60536.0, "PO NO": "911"},
    {"Invoice No": "PY20250930-O", "SKU": "25150015", "Quantity": 61, "Amount": 41663.0, "PO NO": "911"},
]

data_PY20251009_O = [
    {"Invoice No": "PY20251009-O", "SKU": "25150006", "Quantity": 27, "Amount": 17536.5, "PO NO": "913"},
    {"Invoice No": "PY20251009-O", "SKU": "25150007", "Quantity": 27, "Amount": 17617.5, "PO NO": "913"},
    {"Invoice No": "PY20251009-O", "SKU": "25150008", "Quantity": 30, "Amount": 19575.0, "PO NO": "913"},
    {"Invoice No": "PY20251009-O", "SKU": "25150009", "Quantity": 30, "Amount": 20010.0, "PO NO": "913"},
    {"Invoice No": "PY20251009-O", "SKU": "25150010", "Quantity": 20, "Amount": 13540.0, "PO NO": "913"},
    {"Invoice No": "PY20251009-O", "SKU": "23150033", "Quantity": 45, "Amount": 27135.0, "PO NO": "913"},
]

data_PY20251013 = [
    {"Invoice No": "PY20251013", "SKU": "ELCO1037BATT14EB", "Quantity": 99, "Amount": 24255.0, "PO NO": "880"},
]

data_PY20250806_1 = []

#no purchase order but expensive
data_PY20250807_1 = []
#no purchase order but expensive
data_PY20250814_1 = []

data_PY20250826_1 = []

data_PY20250903 = []

data_PY20250905 = []


#VISION

data_PJ25072602 = [
    {"Invoice No": "PJ25072602", "SKU": "25110107", "Quantity": 500.0, "Amount": 175.0, "PO NO": "PO0906"},
    {"Invoice No": "PJ25072602", "SKU": "CTOR011701001", "Quantity": 50.0, "Amount": 1400.0, "PO NO": "PO0882"},
    {"Invoice No": "PJ25072602", "SKU": "25110096", "Quantity": 500.0, "Amount": 2475.0, "PO NO": "PO0903"},
    {"Invoice No": "PJ25072602", "SKU": "25110096", "Quantity": 30.0, "Amount": 148.5, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "FREE003100101", "Quantity": 15.0, "Amount": 72.9, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "0411A03000401", "Quantity": 50.0, "Amount": 225.0, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110131", "Quantity": 20.0, "Amount": 735.0, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110132", "Quantity": 25.0, "Amount": 918.75, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "BRAK008900101", "Quantity": 30.0, "Amount": 535.5, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "BRAK008900201", "Quantity": 50.0, "Amount": 892.5, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "ELCO0002CHAR00V2", "Quantity": 86.0, "Amount": 1452.54, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "24110003", "Quantity": 10.0, "Amount": 1.4, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110135", "Quantity": 10.0, "Amount": 3.5, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "24110010", "Quantity": 5.0, "Amount": 17.4, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "24110006", "Quantity": 10.0, "Amount": 35.2, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "24110005", "Quantity": 10.0, "Amount": 35.2, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110139", "Quantity": 5.0, "Amount": 129.45, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "DISP004001501", "Quantity": 30.0, "Amount": 540.0, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "FREE003100101", "Quantity": 10.0, "Amount": 48.6, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "24110008", "Quantity": 20.0, "Amount": 570.4, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110128", "Quantity": 15.0, "Amount": 217.5, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110130", "Quantity": 15.0, "Amount": 1140.0, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "RMOT000301701-36V", "Quantity": 10.0, "Amount": 850.0, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "23110066", "Quantity": 10.0, "Amount": 760.0, "PO NO": "PO0903"},
    {"Invoice No": "PJ25072602", "SKU": "SHIF004300101", "Quantity": 50.0, "Amount": 175.0, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "DRCH1226COGG0000", "Quantity": 5.0, "Amount": 22.5, "PO NO": "PO0903"},
    {"Invoice No": "PJ25072602", "SKU": "0409A01001301", "Quantity": 20.0, "Amount": 90.0, "PO NO": "PO0903"},
]

data_PJ25072602_two = [
    {"Invoice No": "PJ25072602", "SKU": "ELCO0037BS0401", "Quantity": 50.0, "Amount": 342.5, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "ELCO0002CHAR00V2", "Quantity": 14.0, "Amount": 236.46, "PO NO": "PO0893"},
    {"Invoice No": "PJ25072602", "SKU": "UPS Freight", "Quantity": 1.0, "Amount": 230.0, "PO NO": ""},
]

data_PJ25072602_2 = [
    {"Invoice No": "PJ25072602-2", "SKU": "25110096", "Quantity": 63.0, "Amount": 311.85, "PO NO": "PO0893"},
]

data_ZC25032701_1 = [
    {"Invoice No": "ZC25032701-1", "SKU": "23150052", "Quantity": 790, "Amount": 499280.0, "PO NO": "PO0869"},
    {"Invoice No": "ZC25032701-1", "SKU": "23150051", "Quantity": 150, "Amount": 94800.0, "PO NO": "PO0869"},
]

data_ZC25032701_2 = [
    {"Invoice No": "ZC25032701-2", "SKU": "23150052", "Quantity": 376, "Amount": 237632.0, "PO NO": "PO0869"},
]

data_ZC25032701_3 = [
    {"Invoice No": "ZC25032701-3", "SKU": "23150052", "Quantity": 188, "Amount": 118816.0, "PO NO": "PO0869"},
]

data_ZC25050801_2 = [
    {"Invoice No": "ZC25050801-2", "SKU": "23150052", "Quantity": 414, "Amount": 265788.0, "PO NO": "PO0879"},
    {"Invoice No": "ZC25050801-2", "SKU": "23150051", "Quantity": 150, "Amount": 96300.0, "PO NO": "PO0879"},
]

data_ZC25072501_2 = [
    {"Invoice No": "ZC25072501-2", "SKU": "23150052", "Quantity": 576, "Amount": 363456.0, "PO NO": "PO0895"},
]

#shipping for multiple purchase orders
data_ZY20250917 = []

#sample bikes
data_ZC25032701_4 = []


#PEIYA
df_DF20250620 = pd.DataFrame(DF20250620_data)
df_DF20250624_O = pd.DataFrame(DF20250624_O_data)
df_PY20250826_O = pd.DataFrame(PY20250826_O_data)
df_PY20250903_O = pd.DataFrame(PY20250903_O_data)
df_PY20250905_O = pd.DataFrame(PY20250905_O_data)
df_PY2025920_O = pd.DataFrame(PY2025920_O_data)
df_PY2025929_O = pd.DataFrame(PY2025929_O_data)
df_PY20250806_O = pd.DataFrame(PY20250806_O_data)
df_20250721 = pd.DataFrame(data_20250721)
df_20250825_1 = pd.DataFrame(data_20250825_1)
df_20250825_2 = pd.DataFrame(data_20250825_2)
df_20250825_3 = pd.DataFrame(data_20250825_3)
df_20250825_4 = pd.DataFrame(data_20250825_4)
df_20250912_1 = pd.DataFrame(data_20250912_1)
df_20250912_2 = pd.DataFrame(data_20250912_2)
df_PY20250822_1 = pd.DataFrame(data_PY20250822_1)
df_PY20250822_2 = pd.DataFrame(data_PY20250822_2)
df_PY20250930_O = pd.DataFrame(data_PY20250930_O)
df_PY20251009_O = pd.DataFrame(data_PY20251009_O)
df_PY20251013 = pd.DataFrame(data_PY20251013)


#Vison
df_PJ25072602 = pd.DataFrame(data_PJ25072602)
df_PJ25072602_two = pd.DataFrame(data_PJ25072602_two)
df_PJ25072602_2 = pd.DataFrame(data_PJ25072602_2)
df_ZC25032701_1 = pd.DataFrame(data_ZC25032701_1)
df_ZC25032701_2 = pd.DataFrame(data_ZC25032701_2)
df_ZC25032701_3 = pd.DataFrame(data_ZC25032701_3)
df_ZC25050801_2 = pd.DataFrame(data_ZC25050801_2)
df_ZC25072501_2 = pd.DataFrame(data_ZC25072501_2)

#
# # Save to Excel
# df.to_excel("purchase_order_PO-00000890.xlsx", index=False)