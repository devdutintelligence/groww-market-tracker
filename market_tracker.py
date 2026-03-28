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

exchange_symbols = tuple(f"NSE_{symbol}" for symbol in STOCK_LIST)

# 2. Read the API token from the 'key' file
try:
    with open('key', 'r') as file:
        API_AUTH_TOKEN = file.read().strip()
except FileNotFoundError:
    print("Error: The 'key' file was not found.")
    exit(1)

# 3. Initialize Groww API
print("Initializing Groww API...")
groww = GrowwAPI(API_AUTH_TOKEN)

# 4. Fetch market data
print(f"Fetching data for {len(STOCK_LIST)} stocks...")
live_quotes = {}
ltp_response = {}
ohlc_response = {}

try:
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
            print(f"  -> Warning: Could not fetch quote for {symbol}")
            live_quotes[symbol] = {}

    print("Pulling batched LTP data...")
    ltp_response = groww.get_ltp(
        segment=groww.SEGMENT_CASH,
        exchange_trading_symbols=exchange_symbols
    )

    print("Pulling batched OHLC data...")
    ohlc_response = groww.get_ohlc(
        segment=groww.SEGMENT_CASH,
        exchange_trading_symbols=exchange_symbols
    )
    
    print("All data fetched successfully.")

except Exception as e:
    print(f"Error during batched data fetching: {e}")
    exit(1)

# 5. Structure data into clean, flat CSV columns
csv_filename = 'market_data_50.csv'
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Define the exact column headers we want in the CSV
fieldnames = [
    "Symbol", "Timestamp", "Current_Price (LTP)", "Open", "High", "Low", 
    "Previous_Close", "Day_Change", "Day_Change_Perc", "Volume", 
    "52W_High", "52W_Low", "Upper_Circuit", "Lower_Circuit"
]

csv_rows = []
for symbol in STOCK_LIST:
    exch_symbol = f"NSE_{symbol}"
    
    # Safely get data dictionaries
    quote = live_quotes.get(symbol) or {}
    ltp_data = ltp_response.get(exch_symbol) if isinstance(ltp_response, dict) else None
    ohlc_data = ohlc_response.get(exch_symbol) or {}
    
    # Extract values cleanly. Fallback to "N/A" if something is missing
    row = {
        "Symbol": symbol,
        "Timestamp": current_time,
        "Current_Price (LTP)": ltp_data if ltp_data is not None else quote.get("last_price", "N/A"),
        "Open": ohlc_data.get("open", "N/A"),
        "High": ohlc_data.get("high", "N/A"),
        "Low": ohlc_data.get("low", "N/A"),
        "Previous_Close": ohlc_data.get("close", "N/A"),
        "Day_Change": quote.get("day_change", "N/A"),
        "Day_Change_Perc": quote.get("day_change_perc", "N/A"),
        "Volume": quote.get("volume", "N/A"),
        "52W_High": quote.get("week_52_high", "N/A"),
        "52W_Low": quote.get("week_52_low", "N/A"),
        "Upper_Circuit": quote.get("upper_circuit_limit", "N/A"),
        "Lower_Circuit": quote.get("lower_circuit_limit", "N/A")
    }
    csv_rows.append(row)

# 6. Write to CSV (Using 'w' mode deletes old data and updates with latest)
with open(csv_filename, 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"Success: Market data beautifully structured and saved to {csv_filename}")
