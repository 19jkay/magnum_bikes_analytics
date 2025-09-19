import pandas as pd
from datetime import datetime, timedelta
import re

#input: seriel number
#output: most recent Sales Order (SO) number, product description (whether black or teal)

from AI_Automation.Warehouse_and_Logistics.Costco_Returns.Costco_Returns_Helper import AI_Automation_SalesOrders_clean, create_stock_adjustment

today = datetime.today()
today_str = today.strftime('%Y-%m-%d')

start_date = '2023-01-01'
end_date = today_str

warehouse_code = 'Costco Returns'
quantity = 1

reload_data = False
save_excel = False




# raw_input = input("Paste Excel row here: ")
# raw_items = raw_input.strip().split('\t')
# # Remove parenthetical notes and extra spaces
# data_list = [re.sub(r'\s*\(.*?\)\s*', '', item).strip() for item in raw_items]
# print("Serial numbers received:", data_list)

raw_input = input("Paste Excel row here: ")
raw_items = raw_input.strip().split('\t')

# Extract serial numbers and notes
serials_with_notes = []
data_list = []

for item in raw_items:
    match = re.match(r'\s*(\S+)\s*(?:\((.*?)\))?\s*', item)
    if match:
        serial = match.group(1)
        note = match.group(2) if match.group(2) else ''
        serials_with_notes.append((serial, note))
        data_list.append(serial)

# print("Serial numbers received:", '\t'.join(data_list))
print("Serial numbers received:", data_list)


df_salesorders = AI_Automation_SalesOrders_clean(start_date, end_date, reload=reload_data, save_excel=save_excel)

lost_serial_numbers = []
completed_serial_numbers = []

product_map = {
    'Cosmo 2.0 T - Black- 48v 15 Ah': 'CPO23150052',
    'Cosmo 2.0 T - Calypso - 48v 15 Ah': 'CPO23150051'
}

for serial_number in data_list:
    matches = df_salesorders[df_salesorders['SerialNumber_Identifier'] == serial_number].copy()

    if matches.empty:
        lost_serial_numbers.append(serial_number)
        continue

    if len(matches) > 1:
        # Convert CompletedDate to datetime format
        print(f"Multiple sales orders found for serial number {serial_number}. Working to get most recent serial number....")
        matches['CompletedDate'] = pd.to_datetime(matches['CompletedDate'], format='%Y-%m-%d', errors='coerce')  # Convert CompletedDate to datetime format
        matches = matches.sort_values(by='CompletedDate', ascending=False)  # Sort by CompletedDate descending and take the most recent
        print(f"Found Sales Order {matches['SalesOrderNumber'].iloc[0]} with most recent date {matches['CompletedDate'].iloc[0]}")


    sales_order_number = matches['SalesOrderNumber'].iloc[0]
    product_description = matches['ProductDescription'].iloc[0]
    comment = f"Done with code, SO number: {sales_order_number}"

    product_code = product_map.get(product_description, 'Not Cosmo 2.0')

    if product_code != 'Not Cosmo 2.0':
        # create_stock_adjustment(product_code=product_code, serial_number=serial_number,
        #                         warehouse_code=warehouse_code, quantity=quantity, comment=comment)
        print(f"Stock adjustment done for {serial_number}")
        completed_serial_numbers.append(serial_number)
    else:
        # print(f"⚠️ Serial number {serial_number} is not a Cosmo 2.0 product.")
        lost_serial_numbers.append(serial_number)

# print("✅ Stock adjustment completed for serial numbers:\t" + '\t'.join(completed_serial_numbers))
# print("\n🔍 Lost or unmatched serial numbers:\t" + '\t'.join(lost_serial_numbers))



# Create lookup for notes
note_lookup = {serial: note for serial, note in serials_with_notes}

# Format completed serials with notes
completed_output = [
    f"{serial} ({note_lookup[serial]})" if note_lookup[serial] else serial
    for serial in completed_serial_numbers
]

# Format lost serials with notes
lost_output = [
    f"{serial} ({note_lookup[serial]})" if note_lookup[serial] else serial
    for serial in lost_serial_numbers
]

print("\n✅ Stock adjustment completed for serial numbers:\t" + '\t'.join(completed_output))
print("\n❌ Lost or unmatched serial numbers:\t" + '\t'.join(lost_output))


