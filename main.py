import json
import requests
import pandas as pd
import concurrent.futures
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

myscope = ['https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive']
mycred = ServiceAccountCredentials.from_json_keyfile_name('credentials.json',myscope)

client =gspread.authorize(mycred)

mysheet = client.open("crypto").sheet1

header_format = mysheet.format('A1:G1', {
    'backgroundColor': {'red': 0.0, 'green': 0.5, 'blue': 0.5},
    'textFormat': {
        'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
        'bold': True
    }
})

# Create a session object to reuse the underlying TCP connection
session = requests.Session()

url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
res = session.get(url)
data=json.loads(res.text)
df=pd.DataFrame(data)
df = df.sort_values(by=['lastFundingRate'], ascending=False)
condition1 = df['symbol'].str.contains("USDT")
condition2 = df['symbol'].str.len() < 12
filtered_df = df[condition1 & condition2]
symbols = filtered_df['symbol'].values

# Define a function to fetch the open interest data for a given symbol and period
def get_open_interest_data(symbol, period):
    url = f'https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period={period}&limit=2'
    response = session.get(url)
    if response.ok:
        return response.json()
    else:
        return None

# Define a function to process the open interest data for a given symbol
def process_symbol_data(symbol):
    funding = df.loc[df['symbol'] == symbol, 'lastFundingRate'].values[0]
    data_5m = get_open_interest_data(symbol, '5m')
    data_15m = get_open_interest_data(symbol, '15m')
    data_30m = get_open_interest_data(symbol, '30m')
    data_1h = get_open_interest_data(symbol, '1h')
    data_2h = get_open_interest_data(symbol, '2h')
    if data_5m and len(data_5m) > 1 and data_15m and len(data_15m) > 1:
        try:
            oi_change_5m = round((float(data_5m[1]['sumOpenInterest']) - float(data_5m[0]['sumOpenInterest'])) / float(data_5m[0]['sumOpenInterest']) * 100, 1)
            oi_change_15m = round((float(data_15m[1]['sumOpenInterest']) - float(data_15m[0]['sumOpenInterest'])) / float(data_15m[0]['sumOpenInterest']) * 100, 1)
            oi_change_30m = round((float(data_30m[1]['sumOpenInterest']) - float(data_30m[0]['sumOpenInterest'])) / float(data_30m[0]['sumOpenInterest']) * 100, 1)
            oi_change_1h = round((float(data_1h[1]['sumOpenInterest']) - float(data_1h[0]['sumOpenInterest'])) / float(data_1h[0]['sumOpenInterest']) * 100, 1)
            oi_change_2h = round((float(data_2h[1]['sumOpenInterest']) - float(data_2h[0]['sumOpenInterest'])) / float(data_2h[0]['sumOpenInterest']) * 100, 1)
            row = pd.DataFrame({'symbol': symbol, 'oi_change_5m': oi_change_5m, 'oi_change_15m': oi_change_15m, 'oi_change_30m': oi_change_30m, 'oi_change_1h': oi_change_1h, 'oi_change_2h': oi_change_2h, 'funding': str(round(float(funding) * 100, 5))}, index=[0])
            return row
        except Exception as exc:
            print(f'{symbol} generated an exception: {exc}')
    return None

# Use a ThreadPoolExecutor to process the symbols in parallel
while True:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_symbol_data, symbol) for symbol in symbols]
        results = [future.result() for future in futures if future.result() is not None]

    # Concatenate the results into a single DataFrame
    df2 = pd.concat(results, ignore_index=True)
    df2 = df2.sort_values(by='oi_change_5m',ascending=False)
    mysheet.clear()
    sheet_data = df2.values.tolist()
    header = df2.columns.tolist()
    cell_list = mysheet.range(1, 1, 1+len(sheet_data), len(sheet_data[0]))
    for cell in cell_list:
        if cell.row == 1:
            val = header[cell.col-1]
        else:
            val = sheet_data[cell.row-2][cell.col-1]
        if isinstance(val, str):
            cell.value = val
        else:
            cell.value = str(val)
    mysheet.update_cells(cell_list)
    time.sleep(30)
