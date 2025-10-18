"""
main.py
Automated stock data updater.
Checks the latest date for each ticker and downloads new data daily.
"""
import os
import pandas as pd
import yfinance as yf
import sqlite3
from datetime import datetime, timedelta
from tqdm import tqdm
from dotenv import load_dotenv
import schedule
import time

# === Configuration ===
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/stocks.db")
TICKERS = os.getenv("TICKERS").split(",")

def get_last_date(conn, ticker):
    """Return the latest date for a ticker in the database."""
    query = f"SELECT MAX(Date) FROM stocks_data WHERE ticker='{ticker}'"
    result = conn.execute(query).fetchone()[0]
    return datetime.fromisoformat(result) if result else None

def update_stock_data():
    """Download and append new daily data for each ticker."""
    conn = sqlite3.connect(DB_PATH)
    print("Updating stock data...")

    for ticker in tqdm(TICKERS, desc="Updating tickers"):
        last_date = get_last_date(conn, ticker)
        start_date = last_date + timedelta(days=1) if last_date else datetime(2020, 1, 1)
        end_date = datetime.today()

        if start_date >= end_date:
            continue

        df = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )

        df.reset_index(inplace=True)

        #FIX: flatten MultiIndex columns like ('Open', 'AAPL')
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]


        df.columns = [str(c) for c in df.columns]
        
        
        if df.empty:
            continue

        df.reset_index(inplace=True)
        df["ticker"] = ticker
        df["return_pct"] = (df["Close"] - df["Open"]) / df["Open"] * 100
        df["ma7"] = df["Close"].rolling(7).mean()
        df["volatility"] = df["Close"].rolling(7).std()
        
        # 🧹 remove unwanted 'index' column if present
        if "index" in df.columns:
            df.drop(columns=["index"], inplace=True)
        df.to_sql("stocks_data", conn, if_exists="append", index=False)

    conn.close()
    print("Update complete!")

if __name__ == "__main__":
    update_stock_data()
    schedule.every().day.at("18:00").do(update_stock_data)
    print("Scheduler started — waiting for 18:00 daily update...", flush=True)
    while True:
        schedule.run_pending()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] waiting for next scheduled run...", flush=True)
        time.sleep(60)
