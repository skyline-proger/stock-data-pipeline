"""Database schema utilities for the stock data pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from sqlalchemy.engine import Engine

_SCHEMA_PATH = Path(__file__).resolve().parent / "migrations" / "0001_create_stocks_data.sql"


def _load_schema_statements() -> Iterable[str]:
    """Return the non-empty SQL statements defined in the schema file."""
    raw_sql = _SCHEMA_PATH.read_text(encoding="utf-8")
    for statement in raw_sql.split(";"):
        stmt = statement.strip()
        if stmt:
            yield stmt


def ensure_schema(engine: Engine) -> None:
    """Apply the schema DDL to the provided engine."""
    statements = list(_load_schema_statements())
    if not statements:
        raise RuntimeError(f"No DDL statements found in {_SCHEMA_PATH}")

    with engine.begin() as conn:
        for stmt in statements:
            conn.exec_driver_sql(stmt)
