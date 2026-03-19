from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlite3

from .logging_config import get_logger
from .paths import get_paths

logger = get_logger(__name__)


@dataclass
class CleanConfig:
    min_quantity: int = 1
    min_unit_price: float = 0.0
    drop_missing_customer: bool = True


def load_raw_csv(path: Optional[Path] = None) -> pd.DataFrame:
    """Load the raw retail_sales CSV into a DataFrame."""
    if path is None:
        path = get_paths().raw_dir / "retail_sales.csv"
    df = pd.read_csv(path)
    return df


def clean_sales(df: pd.DataFrame, cfg: Optional[CleanConfig] = None) -> pd.DataFrame:
    """Apply basic cleaning to the raw sales data."""
    cfg = cfg or CleanConfig()

    df = df.copy()

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

    # Normalize to ISO-like strings for SQLite TEXT column (consistent strftime in SQL).
    df["InvoiceDate"] = df["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _init_staging(conn: sqlite3.Connection, *, use_unique_index: bool = False) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_sales_clean (
            InvoiceNo TEXT,
            StockCode TEXT,
            Description TEXT,
            Quantity INTEGER,
            InvoiceDate TEXT,
            UnitPrice REAL,
            CustomerID INTEGER,
            Country TEXT,
            line_total REAL
        )
        """
    )
    if use_unique_index:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_sales_key
            ON stg_sales_clean (InvoiceNo, StockCode, CustomerID, InvoiceDate)
            """
        )


def rebuild_marts(conn: sqlite3.Connection) -> None:
    query_monthly = """
        SELECT
            strftime('%Y-%m', InvoiceDate) AS year_month,
            SUM(line_total) AS revenue,
            SUM(Quantity) AS units,
            COUNT(DISTINCT InvoiceNo) AS invoices
        FROM stg_sales_clean
        GROUP BY year_month
        ORDER BY year_month
    """
    monthly = pd.read_sql_query(query_monthly, conn)
    monthly.to_sql("mart_sales_monthly", conn, if_exists="replace", index=False)

    query_product = """
        SELECT
            StockCode,
            Description,
            SUM(line_total) AS revenue,
            SUM(Quantity) AS units,
            COUNT(DISTINCT InvoiceNo) AS invoices
        FROM stg_sales_clean
        GROUP BY StockCode, Description
        ORDER BY revenue DESC
    """
    product = pd.read_sql_query(query_product, conn)
    product.to_sql("mart_product_summary", conn, if_exists="replace", index=False)

    query_country = """
        SELECT
            Country,
            SUM(line_total) AS revenue,
            SUM(Quantity) AS units,
            COUNT(DISTINCT InvoiceNo) AS invoices,
            COUNT(DISTINCT CustomerID) AS customers
        FROM stg_sales_clean
        GROUP BY Country
        ORDER BY revenue DESC
    """
    country = pd.read_sql_query(query_country, conn)
    country.to_sql("mart_country_summary", conn, if_exists="replace", index=False)

    query_customer = """
        SELECT
            CustomerID,
            SUM(line_total) AS revenue,
            SUM(Quantity) AS units,
            COUNT(DISTINCT InvoiceNo) AS invoices
        FROM stg_sales_clean
        GROUP BY CustomerID
        ORDER BY revenue DESC
    """
    customer = pd.read_sql_query(query_customer, conn)
    customer.to_sql("mart_customer_summary", conn, if_exists="replace", index=False)


def write_sqlite(clean_df: pd.DataFrame, db_path: Optional[Path] = None) -> Path:
    """Write staging and mart tables into a SQLite database (single transaction)."""
    paths = get_paths()
    if db_path is None:
        db_path = paths.db_dir / "retail.db"
    _ensure_parent(db_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DROP TABLE IF EXISTS stg_sales_clean")
        conn.execute("DROP INDEX IF EXISTS ux_sales_key")
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
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR IGNORE INTO stg_sales_clean ({','.join(cols)}) VALUES ({placeholders})"
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
    """Convenience function: load CSV, clean, and write SQLite marts."""
    paths = get_paths()
    if csv_path is None:
        csv_path = paths.raw_dir / "retail_sales.csv"
    if db_path is None:
        db_path = paths.db_dir / "retail.db"

    _ensure_parent(db_path)

    if mode == "full":
        logger.info("ETL full load from %s", csv_path)
        raw_df = load_raw_csv(csv_path)
        clean_df = clean_sales(raw_df, cfg=cfg)
        return write_sqlite(clean_df, db_path=db_path)

    if mode != "incremental":
        raise ValueError("mode must be 'full' or 'incremental'")

    logger.info("ETL incremental load from %s", csv_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _init_staging(conn, use_unique_index=True)
        row = conn.execute("SELECT MAX(InvoiceDate) FROM stg_sales_clean").fetchone()
        last_max = row[0]
        last_max_dt = pd.to_datetime(last_max) if last_max else None

        inserted = 0
        for chunk in pd.read_csv(csv_path, chunksize=chunksize):
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

