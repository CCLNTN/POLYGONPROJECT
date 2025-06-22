import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from polygon import RESTClient

# --- CONFIGURATION SECTION ---
API_KEY = "YOUR_API_KEY"
SYMBOLS = ["AAPL", "NVDA"]  # Replace or load from your 10/50SYMBOLS.txt
START_DATE = "2024-06-01"
END_DATE = "2024-06-05"
OUT_BASE = r"C:\2025\QUOTES\PARQUET"  # or TICKS\PARQUET for trade data
DATA_TYPE = "quotes"  # or "trades"
# --- END OF CONFIGURATION ---

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def fetch_and_save_per_day(symbol, date, out_dir, data_type):
    client = RESTClient(API_KEY)
    date_str = date.strftime("%Y-%m-%d")
    out_path = out_dir / f"{date_str}_{data_type}_{symbol}.parquet"
    if out_path.exists():
        print(f"Already exists: {out_path}")
        return

    if data_type == "quotes":
        try:
            quotes = client.list_quotes(symbol, date_str, date_str)
            data = [q.__dict__ for q in quotes]
        except Exception as e:
            print(f"Error fetching quotes for {symbol} on {date_str}: {e}")
            return
    elif data_type == "trades":
        try:
            trades = client.list_trades(symbol, date_str, date_str)
            data = [t.__dict__ for t in trades]
        except Exception as e:
            print(f"Error fetching trades for {symbol} on {date_str}: {e}")
            return
    else:
        raise ValueError("data_type must be 'quotes' or 'trades'")

    if not data:
        print(f"No data for {symbol} on {date_str}")
        return

    df = pd.DataFrame(data)
    df["symbol"] = symbol
    df["date"] = date_str
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

    for symbol in SYMBOLS:
        for single_date in daterange(start_date, end_date):
            year = single_date.strftime("%Y")
            month = single_date.strftime("%m")
            out_dir = Path(OUT_BASE) / year / month
            out_dir.mkdir(parents=True, exist_ok=True)
            fetch_and_save_per_day(symbol, single_date, out_dir, DATA_TYPE)