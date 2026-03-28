import csv
import os
import time
from datetime import datetime
from growwapi import GrowwAPI

# 1. The list of stocks to track
STOCK_LIST = [
    "HDFCBANK", "BHARTIARTL", "RELIANCE", "ICICIBANK", "SBIN", "ONGC", "INFY", 
    "LT", "HCLTECH", "TCS", "AXISBANK", "SHRIRAMFIN", "KOTAKBANK", "M&M", 
    "MARUTI", "BAJFINANCE", "ETERNAL", "BEL", "POWERGRID", "COALINDIA", "NTPC", 
    "TATASTEEL", "INDIGO", "ITC", "GRASIM", "HINDUNILVR", "TMPV", "SUNPHARMA", 
    "WIPRO", "ADANIENT", "ULTRACEMCO", "JIOFIN", "TRENT", "BAJAJFINSV", "TITAN", 
    "APOLLOHOSP", "EICHERMOT", "TECHM", "ADANIPORTS", "HINDALCO", "JSWSTEEL", 
    "BAJAJ-AUTO", "DRREDDY", "CIPLA", "HDFCLIFE", "MAXHEALTH", "ASIANPAINT", 
    "NESTLEIND", "SBILIFE", "TATACONSUM"
]

# Dynamically create the ("NSE_SYMBOL1", "NSE_SYMBOL2", ...) tuple needed for LTP and OHLC
exchange_symbols = tuple(f"NSE_{symbol}" for symbol in STOCK_LIST)

# 2. Read the API token from the 'key' file
try:
    with open('key', 'r') as file:
        API_AUTH_TOKEN = file.read().strip()
except FileNotFoundError:
    print("Error: The 'key' file was not found in the current directory.")
    exit(1)

# 3. Initialize Groww API
print("Initializing Groww API...")
groww = GrowwAPI(API_AUTH_TOKEN)

# 4. Fetch all market data
print(f"Fetching data for {len(STOCK_LIST)} stocks...")
live_quotes = {}
ltp_response = {}
ohlc_response = {}

try:
    # A. Fetch Live Quotes
    print("Pulling individual live quotes (this may take a moment)...")
    for symbol in STOCK_LIST:
        try:
            live_quotes[symbol] = groww.get_quote(
                exchange=groww.EXCHANGE_NSE,
                segment=groww.SEGMENT_CASH,
                trading_symbol=symbol
            )
            time.sleep(0.1) 
        except Exception as e:
            print(f"  -> Warning: Could not fetch quote for {symbol}: {e}")
            live_quotes[symbol] = None

    # B. Fetch LTP for all instruments at once
    print("Pulling batched LTP data...")
    ltp_response = groww.get_ltp(
        segment=groww.SEGMENT_CASH,
        exchange_trading_symbols=exchange_symbols
    )

    # C. Fetch OHLC for all instruments at once
    print("Pulling batched OHLC data...")
    ohlc_response = groww.get_ohlc(
        segment=groww.SEGMENT_CASH,
        exchange_trading_symbols=exchange_symbols
    )
    
    print("All data fetched successfully.")

except Exception as e:
    print(f"Error during batched data fetching: {e}")
    exit(1)

# 5. Structure data for CSV
csv_filename = 'market_data_50.csv'
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Prepare rows for the CSV
csv_rows = []
for symbol in STOCK_LIST:
    exch_symbol = f"NSE_{symbol}"
    
    # Safely extract data from the batched responses
    ltp_data = ltp_response.get(exch_symbol, {}) if isinstance(ltp_response, dict) else {}
    ohlc_data = ohlc_response.get(exch_symbol, {}) if isinstance(ohlc_response, dict) else {}
    
    row = {
        "Symbol": symbol,
        "Timestamp": current_time,
        "Live_Quote": live_quotes.get(symbol, ""),
        "LTP_Data": ltp_data,
        "OHLC_Data": ohlc_data
    }
    csv_rows.append(row)

# 6. Write to CSV (Using 'w' mode overwrites the file entirely, deleting old data)
fieldnames = ["Symbol", "Timestamp", "Live_Quote", "LTP_Data", "OHLC_Data"]

with open(csv_filename, 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"Success: Market data updated and saved to {csv_filename} (Previous data overwritten)")
