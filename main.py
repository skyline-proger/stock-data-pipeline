"""
main.py
Automated stock data updater.
Checks the latest date for each ticker and downloads new data daily.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import schedule
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tqdm import tqdm

# === Configuration ===
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is required. "
        "Example: postgresql+psycopg2://user:password@host:5432/database"
    )
TICKERS = [ticker.strip().upper() for ticker in os.getenv("TICKERS", "").split(",") if ticker.strip()]
if not TICKERS:
    raise RuntimeError("TICKERS environment variable must contain at least one symbol.")

_ENGINE: Engine | None = None


def get_engine() -> Engine:
    """Create and cache the SQLAlchemy engine for PostgreSQL."""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(DATABASE_URL, future=True)
    return _ENGINE


def get_last_date(engine: Engine, ticker: str) -> datetime | None:
    """Return the latest date for a ticker in the database."""
    with engine.connect() as conn:
        result = conn.execute(
            text('SELECT MAX("Date") FROM stocks_data WHERE ticker = :ticker'),
            {"ticker": ticker},
        ).scalar()

    if isinstance(result, datetime):
        return result
    if isinstance(result, date):
        return datetime.combine(result, datetime.min.time())
    if result:
        return datetime.fromisoformat(str(result))
    return None


def update_stock_data():
    """Download and append new daily data for each ticker."""
    engine = get_engine()
    print("Updating stock data...")

    for ticker in tqdm(TICKERS, desc="Updating tickers"):
        last_date = get_last_date(engine, ticker)
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

        # FIX: flatten MultiIndex columns like ('Open', 'AAPL')
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

        df.columns = [str(c) for c in df.columns]

        if df.empty:
            continue

        df["ticker"] = ticker
        df["return_pct"] = (df["Close"] - df["Open"]) / df["Open"] * 100
        df["ma7"] = df["Close"].rolling(7).mean()
        df["volatility"] = df["Close"].rolling(7).std()

        # 🧹 remove unwanted 'index' column if present
        if "index" in df.columns:
            df.drop(columns=["index"], inplace=True)
        df.to_sql(
            "stocks_data",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    print("Update complete!")


if __name__ == "__main__":
    update_stock_data()
    schedule.every().day.at("18:00").do(update_stock_data)
    print("Scheduler started — waiting for 18:00 daily update...", flush=True)
    while True:
        schedule.run_pending()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] waiting for next scheduled run...", flush=True)
        time.sleep(60)
