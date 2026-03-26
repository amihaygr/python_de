"""טעינת קבצי SQL מתיקיית החבילה."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=128)
def load_sql(name: str) -> str:
    """קורא קובץ .sql מתוך `retail_etl/sql/`."""
    sql_dir = Path(__file__).resolve().parent / "sql"
    path = sql_dir / name
    return path.read_text(encoding="utf-8")

