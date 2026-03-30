"""טעינת CSV, ניקוי, כתיבה ל־SQLite ויצירת טבלאות mart."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlite3

from .settings import get_paths
from .settings import DEFAULT_RETAIL_KAGGLE_FILENAME
from .utils import load_sql

import logging

logger = logging.getLogger(__name__)


@dataclass
class CleanConfig:
    """פרמטרים לניקוי שורות."""

    min_quantity: int = 1
    min_unit_price: float = 0.0
    drop_missing_customer: bool = True


def _strip_optional_index_column(df: pd.DataFrame) -> pd.DataFrame:
    """Some Kaggle exports add a redundant `index` column."""
    if "index" in df.columns:
        return df.drop(columns=["index"])
    return df


def load_raw_csv(path: Optional[Path] = None) -> pd.DataFrame:
    """קורא את קובץ ה־CSV הגולמי ל־DataFrame."""
    if path is None:
        path = get_paths().raw_dir / DEFAULT_RETAIL_KAGGLE_FILENAME
    df = pd.read_csv(path)
    return _strip_optional_index_column(df)


def clean_sales(df: pd.DataFrame, cfg: Optional[CleanConfig] = None) -> pd.DataFrame:
    """מנקה נתוני מכירות: טיפוסים, תאריכים, סינון שורות, שדה line_total."""
    cfg = cfg or CleanConfig()

    df = df.copy()
    df = _strip_optional_index_column(df)

    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], dayfirst=True, errors="coerce")

    df["Description"] = df["Description"].fillna("").astype(str).str.strip()
    df["Country"] = df["Country"].fillna("").astype(str).str.strip()

    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    if cfg.drop_missing_customer:
        df = df.dropna(subset=["CustomerID"])

    df = df[df["Quantity"] >= cfg.min_quantity]
    df = df[df["UnitPrice"] > cfg.min_unit_price]
    df = df[df["InvoiceDate"].notna()]

    df["CustomerID"] = df["CustomerID"].astype(int)

    df["line_total"] = df["Quantity"] * df["UnitPrice"]

    # מחרוזת תאריך אחידה לעמודת TEXT ב־SQLite (תואם ל־strftime בשאילתות)
    df["InvoiceDate"] = df["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Online Retail exports may repeat the same line key; staging unique index requires one row per key.
    df = df.drop_duplicates(
        subset=["InvoiceNo", "StockCode", "CustomerID", "InvoiceDate"],
        keep="first",
    )

    return df


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _dedupe_staging_table(conn: sqlite3.Connection) -> None:
    """Remove duplicate natural keys so CREATE UNIQUE INDEX can succeed."""
    n = conn.execute("SELECT COUNT(*) FROM stg_sales_clean").fetchone()[0]
    if n == 0:
        return
    conn.execute(
        """
        DELETE FROM stg_sales_clean
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM stg_sales_clean
            GROUP BY InvoiceNo, StockCode, CustomerID, InvoiceDate
        )
        """
    )


def _init_staging(conn: sqlite3.Connection, *, use_unique_index: bool = False) -> None:
    conn.executescript(load_sql("etl_init_staging.sql"))
    if use_unique_index:
        _dedupe_staging_table(conn)
        conn.executescript(load_sql("etl_create_unique_index.sql"))


def rebuild_marts(conn: sqlite3.Connection) -> None:
    """מחשב מחדש את כל טבלאות ה־mart מתוך staging."""
    query_monthly = load_sql("marts_monthly.sql")
    monthly = pd.read_sql_query(query_monthly, conn)
    monthly.to_sql("mart_sales_monthly", conn, if_exists="replace", index=False)

    query_product = load_sql("marts_product.sql")
    product = pd.read_sql_query(query_product, conn)
    product.to_sql("mart_product_summary", conn, if_exists="replace", index=False)

    query_country = load_sql("marts_country.sql")
    country = pd.read_sql_query(query_country, conn)
    country.to_sql("mart_country_summary", conn, if_exists="replace", index=False)

    query_customer = load_sql("marts_customer.sql")
    customer = pd.read_sql_query(query_customer, conn)
    customer.to_sql("mart_customer_summary", conn, if_exists="replace", index=False)


def write_sqlite(clean_df: pd.DataFrame, db_path: Optional[Path] = None) -> Path:
    """טעינה מלאה: staging + marts בתוך עסקת SQLite אחת."""
    paths = get_paths()
    if db_path is None:
        db_path = paths.db_dir / "retail.db"
    _ensure_parent(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executescript(load_sql("etl_drop_staging.sql"))
        _init_staging(conn, use_unique_index=False)
        clean_df.to_sql("stg_sales_clean", conn, if_exists="append", index=False)
        rebuild_marts(conn)
        conn.commit()
        logger.info(
            "Full SQLite write committed: %s rows -> %s",
            len(clean_df),
            db_path,
        )
    except Exception:
        conn.rollback()
        logger.exception("SQLite full write failed; rolled back.")
        raise
    finally:
        conn.close()

    return db_path


def _insert_incremental(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cols = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
        "line_total",
    ]
    sql = load_sql("etl_insert_incremental.sql")
    before = conn.total_changes
    conn.executemany(sql, df[cols].itertuples(index=False, name=None))
    after = conn.total_changes
    return after - before


def run_etl(
    csv_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    *,
    mode: str = "full",
    cfg: Optional[CleanConfig] = None,
    chunksize: int = 200_000,
) -> Path:
    """מריץ ETL: מצב full (החלפה) או incremental (הוספה + אינדקס ייחודי)."""
    paths = get_paths()
    if csv_path is None:
        csv_path = paths.raw_dir / DEFAULT_RETAIL_KAGGLE_FILENAME
    if db_path is None:
        db_path = paths.db_dir / "retail.db"

    _ensure_parent(db_path)

    if mode == "full":
        logger.info("ETL full load from %s", csv_path)
        raw_df = load_raw_csv(csv_path)
        clean_df = clean_sales(raw_df, cfg=cfg)
        return write_sqlite(clean_df, db_path=db_path)

    if mode != "incremental":
        raise ValueError("mode חייב להיות 'full' או 'incremental'")

    logger.info("ETL incremental load from %s", csv_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _init_staging(conn, use_unique_index=True)
        row = conn.execute(load_sql("etl_select_max_invoice_date.sql")).fetchone()
        last_max = row[0]
        last_max_dt = pd.to_datetime(last_max) if last_max else None

        inserted = 0
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
            chunk = _strip_optional_index_column(chunk)
            if "InvoiceDate" in chunk.columns and last_max_dt is not None:
                dt = pd.to_datetime(chunk["InvoiceDate"], dayfirst=True, errors="coerce")
                chunk = chunk[dt > last_max_dt]
            cleaned = clean_sales(chunk, cfg=cfg)
            inserted += _insert_incremental(conn, cleaned)

        rebuild_marts(conn)
        conn.commit()
        logger.info("Incremental ETL committed; ~%s row operations.", inserted)
    except Exception:
        conn.rollback()
        logger.exception("Incremental ETL failed; rolled back.")
        raise
    finally:
        conn.close()

    return db_path

