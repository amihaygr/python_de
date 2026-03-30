"""עזרי לוג לשימוש חוזר."""

from __future__ import annotations

import logging
import sys
from functools import lru_cache
from pathlib import Path


def configure_logging(level: str = "INFO") -> None:
    """מגדיר את ה-root logger פעם אחת (קריאות חוזרות רק מעדכנות רמה)."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(numeric)
        return
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


@lru_cache(maxsize=128)
def load_sql(name: str) -> str:
    """Read a .sql file from `retail_etl/sql/`."""
    sql_dir = Path(__file__).resolve().parent / "sql"
    path = sql_dir / name
    return path.read_text(encoding="utf-8")

