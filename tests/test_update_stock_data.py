from pathlib import Path
import sys

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import update_stock_data
from schema import ensure_schema


def fake_fetcher(_ticker, _start, _end):
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "Open": [10.0, 11.0],
            "High": [11.0, 12.0],
            "Low": [9.0, 10.5],
            "Close": [10.5, 12.0],
            "Volume": [1000, 1500],
        }
    )


def build_engine():
    return create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def test_update_stock_data_is_idempotent():
    engine = build_engine()
    ensure_schema(engine)

    # First run populates the database
    update_stock_data(engine=engine, tickers=["AAPL"], fetcher=fake_fetcher)

    with engine.connect() as conn:
        initial_count = conn.execute(text("SELECT COUNT(*) FROM stocks_data")).scalar()

    # Second run should not duplicate rows even if the fetcher returns the same data
    update_stock_data(engine=engine, tickers=["AAPL"], fetcher=fake_fetcher)

    with engine.connect() as conn:
        refreshed_count = conn.execute(text("SELECT COUNT(*) FROM stocks_data")).scalar()
        rows = conn.execute(
            text(
                "SELECT date, return_pct, ma7, volatility FROM stocks_data WHERE ticker = :ticker ORDER BY date"
            ),
            {"ticker": "AAPL"},
        ).fetchall()

    assert initial_count == 2
    assert refreshed_count == initial_count

    # Validate derived metrics for the most recent row
    latest = rows[-1]
    assert latest[1] == pytest.approx((12.0 - 11.0) / 11.0 * 100)
    assert latest[2] == pytest.approx((10.5 + 12.0) / 2)
    assert latest[3] is None or latest[3] >= 0
