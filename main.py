"""
main.py
Automated stock data updater.
Checks the latest date for each ticker and downloads new data daily.
"""

from __future__ import annotations

import logging
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

# Local imports
from schema import ensure_schema

# === Configuration ===
load_dotenv()

# === Logging configuration ===
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
_TICKERS_ENV = os.getenv("TICKERS", "")
TICKERS = [ticker.strip().upper() for ticker in _TICKERS_ENV.split(",") if ticker.strip()]

DEFAULT_START_DATE = datetime(2020, 1, 1)

_ENGINE: Engine | None = None


def get_engine() -> Engine:
    """Create and cache the SQLAlchemy engine for PostgreSQL."""
    global _ENGINE
    if _ENGINE is None:
        if not DATABASE_URL:
            raise RuntimeError(
                "DATABASE_URL environment variable is required. "
                "Example: postgresql+psycopg2://user:password@host:5432/database"
            )
        _ENGINE = create_engine(DATABASE_URL, future=True)
    return _ENGINE


def get_last_date(engine: Engine, ticker: str) -> datetime | None:
    """Return the latest date for a ticker in the database."""
    ensure_schema(engine)
    with engine.connect() as conn:
        result = conn.execute(
            text('SELECT MAX(date) FROM stocks_data WHERE ticker = :ticker'),
            {"ticker": ticker},
        ).scalar()

    if isinstance(result, datetime):
        return result
    if isinstance(result, date):
        return datetime.combine(result, datetime.min.time())
    if result:
        return datetime.fromisoformat(str(result))
    return None


def fetch_stock_history(ticker: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Download daily OHLCV history for a ticker from Yahoo Finance."""
    df = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=True,
    )
    if df.empty:
        return df
    return df.reset_index()


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and ensure datetime typing for downstream use."""
    if df.empty:
        return df

    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    if "date" not in df.columns:
        raise ValueError("Expected 'date' column after normalization")

    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.normalize()
    df = df.dropna(subset=["date"])
    df.sort_values("date", inplace=True)
    return df


def update_stock_data(
    *,
    engine: Engine | None = None,
    tickers: list[str] | None = None,
    fetcher=fetch_stock_history,
) -> None:
    """Download and append new daily data for each ticker."""

    engine = engine or get_engine()
    ensure_schema(engine)
    tickers = tickers or TICKERS
    if not tickers:
        raise RuntimeError("No tickers configured. Provide tickers argument or set TICKERS env variable.")
    fetcher = fetcher or fetch_stock_history

    logger.info("Updating stock data for %d ticker(s)", len(tickers))

    for ticker in tqdm(tickers, desc="Updating tickers"):
        logger.debug("Processing %s", ticker)
        last_date = get_last_date(engine, ticker)
        start_date = last_date + timedelta(days=1) if last_date else DEFAULT_START_DATE
        end_date = datetime.today()

        if start_date >= end_date:
            logger.debug("Skipping %s – start date %s is after end date %s", ticker, start_date, end_date)
            continue

        raw_df = fetcher(ticker, start_date, end_date)
        if raw_df is None or getattr(raw_df, "empty", False):
            logger.info("No new rows returned for %s", ticker)
            continue

        df = _normalize_dataframe(raw_df)

        if df.empty:
            logger.info("No new rows returned for %s", ticker)
            continue

        existing_dates = pd.read_sql_query(
            text("SELECT date FROM stocks_data WHERE ticker = :ticker"),
            con=engine,
            params={"ticker": ticker},
        )

        if not existing_dates.empty:
            existing_dates_series = pd.to_datetime(existing_dates["date"]).dt.normalize()
            df = df[~df["date"].isin(existing_dates_series)]

        df.drop_duplicates(subset=["date"], inplace=True)

        if df.empty:
            logger.info("All rows for %s already present", ticker)
            continue

        df["ticker"] = ticker
        df["return_pct"] = (df["close"] - df["open"]) / df["open"] * 100
        df["ma7"] = df["close"].rolling(7, min_periods=1).mean()
        df["volatility"] = df["close"].rolling(7, min_periods=2).std()

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

        logger.info("Inserted %d new row(s) for %s", len(df.index), ticker)

    logger.info("Stock data update complete")


if __name__ == "__main__":
    update_stock_data()
    schedule.every().day.at("18:00").do(update_stock_data)
    logger.info("Scheduler started — waiting for 18:00 daily update...")
    while True:
        schedule.run_pending()
        logger.debug("Waiting for next scheduled run at 18:00")
        time.sleep(60)
