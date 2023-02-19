import json
import requests
import pandas as pd
import numpy as np
import concurrent.futures
import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
import time
import dconfig

discord = dconfig.url
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


url = 'https://fapi.binance.com/fapi/v1/premiumIndex'
res = requests.get(url)
data=json.loads(res.text)
df=pd.DataFrame(data)
df = df.sort_values(by=['lastFundingRate'], ascending=False)
condition1 = df['symbol'].str.contains("USDT")
condition2 = df['symbol'].str.len() < 12
filtered_df = df[condition1 & condition2]
filtered_df = filtered_df[filtered_df['symbol'] != 'BNXUSDT']
symbols = filtered_df['symbol'].values

# Define a function to fetch the open interest data for a given symbol and period
def get_open_interest_data(symbol, period, session):
    url = f'https://www.binance.com/futures/data/openInterestHist?symbol={symbol}&period={period}&limit=2'
    response = session.get(url)
    if response.ok:
        return response.json()
    else:
        return None
    
def get_top_long_short_ratio(symbol, period , session):
    url = f'https://www.binance.com/futures/data/topLongShortAccountRatio?symbol={symbol}&period={period}&limit=2'
    response = session.get(url)
    if response.ok:
        return response.json()
    else:
        return None

# Define a function to process the open interest data for a given symbol
def process_symbol_data(symbol, session):
    funding = df.loc[df['symbol'] == symbol, 'lastFundingRate'].values[0]
    data_5m = get_open_interest_data(symbol, '5m', session)
    data_15m = get_open_interest_data(symbol, '15m', session)
    data_30m = get_open_interest_data(symbol, '30m', session)
    data_1h = get_open_interest_data(symbol, '1h', session)
    data_2h = get_open_interest_data(symbol, '2h', session)
    long_short_data = get_top_long_short_ratio(symbol, '5m', session)
    if data_5m and len(data_5m) > 1 and data_15m and len(data_15m) > 1 and data_30m and len(data_30m) > 1 and data_1h and len(data_1h) > 1 and data_2h and len(data_2h) > 1 and long_short_data and len(long_short_data) > 1:  
        try:
            oi_change_5m = round((float(data_5m[1]['sumOpenInterest']) - float(data_5m[0]['sumOpenInterest'])) / float(data_5m[0]['sumOpenInterest']) * 100, 1)
            oi_change_15m = round((float(data_15m[1]['sumOpenInterest']) - float(data_15m[0]['sumOpenInterest'])) / float(data_15m[0]['sumOpenInterest']) * 100, 1)
            oi_change_30m = round((float(data_30m[1]['sumOpenInterest']) - float(data_30m[0]['sumOpenInterest'])) / float(data_30m[0]['sumOpenInterest']) * 100, 1)
            oi_change_1h = round((float(data_1h[1]['sumOpenInterest']) - float(data_1h[0]['sumOpenInterest'])) / float(data_1h[0]['sumOpenInterest']) * 100, 1)
            oi_change_2h = round((float(data_2h[1]['sumOpenInterest']) - float(data_2h[0]['sumOpenInterest'])) / float(data_2h[0]['sumOpenInterest']) * 100, 1)
            long_short_ratio = float(long_short_data[1]['longShortRatio'])
            long_account = float(long_short_data[1]['longAccount'])
            short_account = float(long_short_data[1]['shortAccount'])
            row = pd.DataFrame({'symbol': symbol, 'oi_change_5m': oi_change_5m, 'oi_change_15m': oi_change_15m, 'oi_change_30m': oi_change_30m, 'oi_change_1h': oi_change_1h, 'oi_change_2h': oi_change_2h, 'funding': str(round(float(funding) * 100, 5)), 'long_short_ratio': long_short_ratio, 'long_account': long_account, 'short_account': short_account}, index=[0])
            return row
        except Exception as exc:
            print(f'{symbol} generated an exception: {exc}')
    return None



# Use a ThreadPoolExecutor to process the symbols in parallel
# Use a ThreadPoolExecutor to process the symbols in parallel
count = 0
while True:
    if count % 6 == 0:
      session = requests.Session() 
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_symbol_data, symbol, session) for symbol in symbols]
        results = [future.result() for future in futures if future.result() is not None]
    if results:
        df2 = pd.concat(results, ignore_index=True)
    else:
        df2 = pd.DataFrame()
    df2 = df2.sort_values(by='oi_change_5m', ascending=False)
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
    condition1 = df2['long_short_ratio'] < 1
    condition2 = df2['funding'].astype(np.float64) != 0.01
    data = {"content": "=======================\nShort\n"+"\n".join([f'{i+1}. {x}' for i, x in enumerate(df2[condition1 & condition2]['symbol'].values)])+"\n======================="}
    requests.post(discord, json=data)
    count += 1
    time.sleep(100)
    session = requests.Session()
