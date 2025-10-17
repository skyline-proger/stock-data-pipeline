import os
import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv

# === Настройки ===
load_dotenv()
DB_PATH = os.getenv("DB_PATH", "data/stocks.db")
TICKERS = os.getenv("TICKERS").split(",")
START_DATE = os.getenv("BACKFILL_START", "2020-01-01")

os.makedirs("data", exist_ok=True)


def backfill():
    conn = sqlite3.connect(DB_PATH)
    all_data = []

    print("📊 Starting historical backfill (clean version)...")

    for ticker in tqdm(TICKERS, desc="Downloading stocks"):
        df = yf.download(
            ticker,
            start=START_DATE,
            end=datetime.today().strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,  # включаем автокоррекцию цен
        )

        if df.empty:
            print(f"⚠️ Skipped {ticker} — no data.")
            continue

        # Убираем мультииндекс полностью
        df.columns = [str(col[0]) if isinstance(col, tuple) else str(col) for col in df.columns]

        # Берём только существующие столбцы (без Adj Close)
        cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
        df = df[cols].copy()

        df.reset_index(inplace=True)
        df["ticker"] = ticker
        df["return_pct"] = (df["Close"] - df["Open"]) / df["Open"] * 100
        df["ma7"] = df["Close"].rolling(7).mean()
        df["volatility"] = df["Close"].rolling(7).std()

        all_data.append(df)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.columns = [c.replace(" ", "_") for c in combined.columns]
        combined.to_sql("stocks_data", conn, if_exists="replace", index=False)
        print(f"✅ Data saved successfully to {DB_PATH}")
    else:
        print("⚠️ No data downloaded!")

    conn.close()


if __name__ == "__main__":
    backfill()
