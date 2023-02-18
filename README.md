## Results

The results of the analysis are stored in a Google Sheet. You can access the sheet at this link:

https://docs.google.com/spreadsheets/d/1q0oKxIcRvUd0HOjWnbzKsgBJlwm58_kcgZ-9nCXms18/edit?usp=sharing

# Binance Open Interest Monitor

This script leverages the Binance API to fetch and analyze open interest data and funding rates for various futures markets on Binance. To improve performance, the script uses multithreading to simultaneously fetch data for multiple markets. Once the data is collected, the script outputs the results to a Google Sheet for easy analysis. By analyzing the open interest changes and funding rates of different markets, traders can identify potential market trends and make informed trading decisions.

## Requirements

To run this script, you will need:

- A Binance Futures API key
- A Google Cloud Platform project with the Google Sheets API enabled
- Python 3.x with the following modules:
    - requests
    - pandas
    - gspread
    - oauth2client
    - concurrent.futures

## Installation

1. Clone this repository.
2. Install the required modules
4. Download a JSON key file for a Google Cloud Platform service account and save it as `credentials.json` in the project directory.

## Usage

1. Run the script with `python main.py`.
2. The script will continuously update the Google Sheet with the latest open interest data every 30 seconds.
