"""Utility to migrate existing SQLite stock data into PostgreSQL."""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy an existing stocks_data table from SQLite into PostgreSQL."
    )
    parser.add_argument(
        "--sqlite-path",
        required=True,
        help="Path to the legacy SQLite database file (e.g. /path/to/stocks.db).",
    )
    parser.add_argument(
        "--table",
        default="stocks_data",
        help="Name of the table to copy (default: stocks_data).",
    )
    parser.add_argument(
        "--if-exists",
        choices=["fail", "replace", "append"],
        default="append",
        help="Behaviour if the destination table already exists (default: append).",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1000,
        help="Number of rows per batch when writing to PostgreSQL (default: 1000).",
    )
    return parser.parse_args()


def require_database_url() -> str:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL environment variable is required. "
            "Example: postgresql+psycopg2://user:password@localhost:5432/stocks"
        )
    return database_url


def build_sqlite_url(sqlite_path: str) -> str:
    path = Path(sqlite_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"SQLite database not found: {path}")
    return f"sqlite:///{path}"


def create_postgres_engine(database_url: str) -> Engine:
    return create_engine(database_url, future=True)


def main() -> None:
    args = parse_args()
    database_url = require_database_url()
    sqlite_url = build_sqlite_url(args.sqlite_path)

    sqlite_engine = create_engine(sqlite_url, future=True)
    postgres_engine = create_postgres_engine(database_url)

    print(f"Reading {args.table!r} from {sqlite_url}...")
    df = pd.read_sql_table(args.table, con=sqlite_engine)
    if df.empty:
        raise RuntimeError(
            "The source table is empty; nothing to migrate. "
            "Verify the SQLite database contains data."
        )

    # Normalise column names and types
    df.columns = [str(col).strip().replace(" ", "_") for col in df.columns]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.upper()

    subset_cols = [col for col in ("ticker", "date") if col in df.columns]
    if subset_cols:
        df = df.drop_duplicates(subset=subset_cols + ["close"] if "close" in df.columns else subset_cols)

    if "return_pct" not in df.columns and {"close", "open"}.issubset(df.columns):
        df["return_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    grouped_close = None
    if "close" in df.columns:
        if {"ticker", "date"}.issubset(df.columns):
            df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
            grouped_close = df.groupby("ticker")["close"]
        else:
            grouped_close = df["close"]

    if "ma7" not in df.columns and grouped_close is not None:
        if hasattr(grouped_close, "transform"):
            df["ma7"] = grouped_close.transform(lambda s: s.rolling(7).mean())
        else:
            df["ma7"] = grouped_close.rolling(7).mean()

    if "volatility" not in df.columns and grouped_close is not None:
        if hasattr(grouped_close, "transform"):
            df["volatility"] = grouped_close.transform(lambda s: s.rolling(7).std())
        else:
            df["volatility"] = grouped_close.rolling(7).std()

    print(
        "Writing {rows} rows to PostgreSQL table {table!r} (if_exists={mode})...".format(
            rows=len(df), table=args.table, mode=args.if_exists
        )
    )
    df.to_sql(
        args.table,
        con=postgres_engine,
        if_exists=args.if_exists,
        index=False,
        method="multi",
        chunksize=args.chunksize,
    )

    print("Migration completed successfully.")


if __name__ == "__main__":
    main()
