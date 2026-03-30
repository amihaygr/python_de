"""ייצוא טבלאות mart מ־SQLite לפורמטים שונים."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import sqlite3

from .settings import get_paths
from .db_security import assert_export_table


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_table(conn: sqlite3.Connection, table: str) -> pd.DataFrame:
    safe = assert_export_table(table)
    return pd.read_sql_query(f"SELECT * FROM {safe}", conn)


def export_tables(
    db_path: Path | None = None,
    tables: Iterable[str] | None = None,
    formats: Iterable[str] | None = None,
) -> None:
    """כותב כל טבלה לפורמטים שנבחרו (למשל CSV, Parquet)."""
    paths = get_paths()
    if db_path is None:
        db_path = paths.db_dir / "retail.db"

    if tables is None:
        tables = (
            "mart_sales_monthly",
            "mart_product_summary",
            "mart_country_summary",
            "mart_customer_summary",
        )

    if formats is None:
        formats = ("csv", "json", "parquet", "xlsx")

    _ensure_parent(paths.exports_dir)

    with sqlite3.connect(db_path) as conn:
        for table in tables:
            df = _read_table(conn, table)
            for fmt in formats:
                export_path = paths.exports_dir / f"{table}.{fmt}"
                _ensure_parent(export_path)
                if fmt == "csv":
                    df.to_csv(export_path, index=False)
                elif fmt == "json":
                    df.to_json(export_path, orient="records", lines=False)
                elif fmt == "parquet":
                    df.to_parquet(export_path, index=False)
                elif fmt == "xlsx":
                    df.to_excel(export_path, index=False)
