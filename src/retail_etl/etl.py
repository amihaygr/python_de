from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlite3

from .paths import get_paths


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

    df["Description"] = df["Description"].astype(str).str.strip()
    df["Country"] = df["Country"].astype(str).str.strip()

    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["UnitPrice"] = pd.to_numeric(df["UnitPrice"], errors="coerce")

    if cfg.drop_missing_customer:
        df = df.dropna(subset=["CustomerID"])

    df = df[df["Quantity"] >= cfg.min_quantity]
    df = df[df["UnitPrice"] > cfg.min_unit_price]
    df = df[df["InvoiceDate"].notna()]

    df["CustomerID"] = df["CustomerID"].astype(int)

    df["line_total"] = df["Quantity"] * df["UnitPrice"]

    return df


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_sqlite(clean_df: pd.DataFrame, db_path: Optional[Path] = None) -> Path:
    """Write staging and mart tables into a SQLite database."""
    paths = get_paths()
    if db_path is None:
        db_path = paths.db_dir / "retail.db"
    _ensure_parent(db_path)

    with sqlite3.connect(db_path) as conn:
        raw_table = "stg_sales_clean"
        clean_df.to_sql(raw_table, conn, if_exists="replace", index=False)

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

    return db_path


def run_etl(csv_path: Optional[Path] = None, db_path: Optional[Path] = None) -> Path:
    """Convenience function: load CSV, clean, and write SQLite marts."""
    raw_df = load_raw_csv(csv_path)
    clean_df = clean_sales(raw_df)
    db_path = write_sqlite(clean_df, db_path=db_path)
    return db_path

