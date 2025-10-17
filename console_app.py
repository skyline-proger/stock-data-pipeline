"""
console_app.py
Interactive console for exploring stock data and visualizing performance.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from datetime import datetime

DB_PATH = "data/stocks.db"

def get_tickers(conn):
    query = "SELECT DISTINCT ticker FROM stocks_data"
    return [row[0] for row in conn.execute(query).fetchall()]

def load_data(conn, ticker, start_date, end_date):
    query = """
    SELECT Date, Open, High, Low, Close, Volume, return_pct, ma7, volatility
    FROM stocks_data
    WHERE ticker = ? AND Date BETWEEN ? AND ?
    ORDER BY Date
    """
    df = pd.read_sql_query(query, conn, params=(ticker, start_date, end_date))
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def show_summary(df):
    avg_return = df["return_pct"].mean()
    avg_volatility = df["volatility"].mean()
    total_change = (df["Close"].iloc[-1] - df["Close"].iloc[0]) / df["Close"].iloc[0] * 100
    print("\n📊 Summary statistics:")
    print(tabulate([
        ["Average daily return (%)", f"{avg_return:.3f}"],
        ["Average volatility", f"{avg_volatility:.3f}"],
        ["Total change over period (%)", f"{total_change:.2f}"]
    ], headers=["Metric", "Value"], tablefmt="grid"))

def plot_data(df, ticker):
    plt.figure(figsize=(10,5))
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
    conn = sqlite3.connect(DB_PATH)

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

    conn.close()
    print("\n✅ Done.")

if __name__ == "__main__":
    main()
