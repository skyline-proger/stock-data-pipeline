"""
historical_loader.py
Initial backfill script for stock data.
Downloads historical data for multiple tickers and stores it in PostgreSQL.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tqdm import tqdm

from schema import ensure_schema

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

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
START_DATE = os.getenv("BACKFILL_START", "2020-01-01")

_ENGINE: Engine | None = None


def get_engine() -> Engine:
    """Create and cache the SQLAlchemy engine for PostgreSQL."""
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(DATABASE_URL, future=True)
    return _ENGINE


def backfill():
    """Download full historical data and write to database."""
    engine = get_engine()
    ensure_schema(engine)
    all_data = []

    logger.info("Starting historical backfill")

    for ticker in tqdm(TICKERS, desc="Downloading stocks"):
        df = yf.download(
            ticker,
            start=START_DATE,
            end=datetime.today().strftime("%Y-%m-%d"),
            progress=False,
            auto_adjust=True,
        )

        if df.empty:
            logger.warning("Skipped %s — no data returned", ticker)
            continue

        df.columns = [str(col[0]) if isinstance(col, tuple) else str(col) for col in df.columns]
        cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
        df = df[cols].copy()

        df.reset_index(inplace=True)
        df["ticker"] = ticker
        df["return_pct"] = (df["close"] - df["open"]) / df["open"] * 100
        df["ma7"] = df["close"].rolling(7).mean()
        df["volatility"] = df["close"].rolling(7).std()

        all_data.append(df)

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.columns = [c.replace(" ", "_") for c in combined.columns]

        combined["return_pct"] = (combined["close"] - combined["open"]) / combined["open"] * 100
        combined["ma7"] = combined.groupby("ticker")["close"].transform(lambda s: s.rolling(7, min_periods=1).mean())
        combined["volatility"] = combined.groupby("ticker")["close"].transform(
            lambda s: s.rolling(7, min_periods=2).std()
        )

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM stocks_data"))

        combined.to_sql(
            "stocks_data",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        logger.info("Historical backfill complete: inserted %d rows", len(combined.index))
    else:
        logger.warning("No data downloaded during backfill")


if __name__ == "__main__":
    backfill()
