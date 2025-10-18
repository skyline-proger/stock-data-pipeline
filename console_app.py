"""
console_app.py
Interactive console for exploring stock data and visualizing performance.
"""

from __future__ import annotations

import os
from datetime import datetime

import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tabulate import tabulate

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is required. "
        "Example: postgresql+psycopg2://user:password@host:5432/database"
    )

_ENGINE: Engine | None = None


def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(DATABASE_URL, future=True)
    return _ENGINE


def get_tickers(conn) -> list[str]:
    query = text("SELECT DISTINCT ticker FROM stocks_data ORDER BY ticker")
    return [row[0] for row in conn.execute(query).fetchall()]


def load_data(conn, ticker, start_date, end_date):
    query = text(
        """
        SELECT "Date", "Open", "High", "Low", "Close", "Volume", return_pct, ma7, volatility
        FROM stocks_data
        WHERE ticker = :ticker AND "Date" BETWEEN :start AND :end
        ORDER BY "Date"
        """
    )
    df = pd.read_sql(query, conn, params={"ticker": ticker, "start": start_date, "end": end_date})
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def show_summary(df):
    avg_return = df["return_pct"].mean()
    avg_volatility = df["volatility"].mean()
    total_change = (df["Close"].iloc[-1] - df["Close"].iloc[0]) / df["Close"].iloc[0] * 100
    print("\n📊 Summary statistics:")
    print(
        tabulate(
            [
                ["Average daily return (%)", f"{avg_return:.3f}"],
                ["Average volatility", f"{avg_volatility:.3f}"],
                ["Total change over period (%)", f"{total_change:.2f}"],
            ],
            headers=["Metric", "Value"],
            tablefmt="grid",
        )
    )


def plot_data(df, ticker):
    plt.figure(figsize=(10, 5))
    plt.plot(df["Date"], df["Close"], label="Close", linewidth=2)
    plt.plot(df["Date"], df["ma7"], label="MA(7)", linestyle="--")
    plt.title(f"{ticker} — Price over Time")
    plt.xlabel("Date")
    plt.ylabel("Price ($)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def main():
    engine = get_engine()
    with engine.connect() as conn:
        print("\n=== 📈 Stock Data Console ===")
        tickers = get_tickers(conn)
        print("Available tickers:")
        print(", ".join(tickers))

        ticker = input("\nEnter ticker symbol: ").strip().upper()
        if ticker not in tickers:
            print("❌ Unknown ticker.")
            return

        start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        end_date = input("Enter end date (YYYY-MM-DD): ").strip()

        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            print("❌ Invalid date format.")
            return

        df = load_data(conn, ticker, start_date, end_date)
        if df.empty:
            print("⚠️ No data for this period.")
            return

        show_summary(df)

        show_plot = input("\nShow price chart? (y/n): ").lower()
        if show_plot == "y":
            plot_data(df, ticker)

    print("\n✅ Done.")


if __name__ == "__main__":
    main()
