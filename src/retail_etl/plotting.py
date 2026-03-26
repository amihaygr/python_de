"""יצירת קבצי גרף Plotly (HTML/PNG) מתוך טבלאות mart."""

from __future__ import annotations

from pathlib import Path

import plotly.express as px
import sqlite3
import pandas as pd

from .paths import get_paths
from .db_security import assert_export_table


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _read_table(db_path: Path, table: str) -> pd.DataFrame:
    safe = assert_export_table(table)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {safe}", conn)


def generate_charts(db_path: Path | None = None) -> None:
    """שומר גרפים עיקריים לתיקיית הדוחות."""
    paths = get_paths()
    if db_path is None:
        db_path = paths.db_dir / "retail.db"
    charts_dir = paths.charts_dir
    charts_dir.mkdir(parents=True, exist_ok=True)

    monthly = _read_table(db_path, "mart_sales_monthly")
    fig_monthly = px.line(
        monthly,
        x="year_month",
        y="revenue",
        title="הכנסה חודשית לאורך זמן",
    )
    _save_figure(fig_monthly, charts_dir / "monthly_revenue")

    products = _read_table(db_path, "mart_product_summary").head(10)
    fig_products = px.bar(
        products,
        x="Description",
        y="revenue",
        title="10 המוצרים המובילים לפי הכנסה",
    )
    fig_products.update_layout(xaxis_title="מוצר", xaxis_tickangle=-45)
    _save_figure(fig_products, charts_dir / "top_products")

    customers = _read_table(db_path, "mart_customer_summary").head(10)
    fig_customers = px.bar(
        customers,
        x="CustomerID",
        y="revenue",
        title="10 הלקוחות המובילים לפי הכנסה",
    )
    _save_figure(fig_customers, charts_dir / "top_customers")

    countries = _read_table(db_path, "mart_country_summary").head(10)
    fig_countries = px.bar(
        countries,
        x="Country",
        y="revenue",
        title="10 המדינות המובילות לפי הכנסה",
    )
    fig_countries.update_layout(xaxis_tickangle=-45)
    _save_figure(fig_countries, charts_dir / "top_countries")


def _save_figure(fig, base_path: Path) -> None:
    html_path = base_path.with_suffix(".html")
    png_path = base_path.with_suffix(".png")
    _ensure_parent(html_path)
    fig.write_html(html_path)
    fig.write_image(png_path, format="png", scale=2)

