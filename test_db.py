"""Integration test for validating an externally provided database connection."""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    pytest.skip("DATABASE_URL environment variable not provided", allow_module_level=True)


def test_database_connection_round_trip():
    engine = create_engine(DATABASE_URL, future=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar()

    assert result == 1