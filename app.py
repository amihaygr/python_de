from __future__ import annotations

import os
import traceback
from pathlib import Path

import pandas as pd
import plotly.express as px
import sqlite3
import streamlit as st

from retail_etl.logging_config import configure_logging
from retail_etl.meta import connect as meta_connect, get_last_success, get_source_state, list_active_alerts
from retail_etl.monitor import check_for_update
from retail_etl.settings import Settings
from retail_etl.sql_guard import assert_read_table


def _default_kaggle_dataset() -> str:
    try:
        sec = st.secrets
        if hasattr(sec, "get"):
            v = sec.get("RETAIL_KAGGLE_DATASET", "")
            return str(v) if v else ""
    except Exception:
        pass
    return os.environ.get("RETAIL_KAGGLE_DATASET", "").strip()


def _default_kaggle_filename() -> str:
    try:
        sec = st.secrets
        if hasattr(sec, "get"):
            v = sec.get("RETAIL_KAGGLE_FILENAME", "")
            if v:
                return str(v)
    except Exception:
        pass
    return os.environ.get("RETAIL_KAGGLE_FILENAME", "retail_sales.csv").strip() or "retail_sales.csv"


def get_connection(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=30.0)


@st.cache_data(ttl=60)
def load_table(db_path: Path, table: str) -> pd.DataFrame:
    safe = assert_read_table(table)
    with get_connection(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {safe}", conn)


@st.cache_data(ttl=60)
def load_dataset_overview(db_path: Path) -> dict:
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS rows_count,
                COUNT(DISTINCT CustomerID) AS customers,
                COUNT(DISTINCT Country) AS countries,
                MIN(InvoiceDate) AS min_date,
                MAX(InvoiceDate) AS max_date
            FROM stg_sales_clean
            """
        ).fetchone()
    return {
        "rows_count": int(row[0]),
        "customers": int(row[1]),
        "countries": int(row[2]),
        "min_date": row[3],
        "max_date": row[4],
    }


def main() -> None:
    configure_logging(os.environ.get("RETAIL_ETL_LOG_LEVEL", "INFO"))

    st.set_page_config(
        page_title="Retail ETL | Analytics",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    settings = Settings.load()
    paths = get_paths()
    db_path = settings.db_path

    st.title("Retail sales — ETL dashboard")
    st.caption("Production-style monitoring, safe SQL access, Plotly visuals.")

    if not db_path.exists():
        st.error(
            f"SQLite database not found at `{db_path}`. "
            "Run `python -m retail_etl.cli run-all` (or set `RETAIL_ETL_DB_PATH`)."
        )
        return

    st.sidebar.header("Configuration")
    dataset = st.sidebar.text_input(
        "Dataset (owner/dataset)",
        value=_default_kaggle_dataset(),
        placeholder="e.g. bekkarmerwan/retail-sales-dataset-sample-transactions",
    )
    filename = st.sidebar.text_input("Filename", value=_default_kaggle_filename())
    allow_incremental = st.sidebar.checkbox("Allow incremental refresh when safe", value=True)
    download_from_kaggle = st.sidebar.checkbox(
        "Pull latest from Kaggle before check",
        value=False,
        help="Requires Kaggle credentials. Overwrites local raw file.",
    )

    with st.spinner("Loading mart tables…"):
        try:
            monthly = load_table(db_path, "mart_sales_monthly")
            products = load_table(db_path, "mart_product_summary")
            customers = load_table(db_path, "mart_customer_summary")
            countries = load_table(db_path, "mart_country_summary")
        except Exception as e:  # noqa: BLE001
            st.error(f"Failed to load mart tables: {e}")
            return

    tab_intro, tab_overview, tab_products, tab_customers, tab_countries = st.tabs(
        ["Intro", "Overview", "Products", "Customers", "Countries"]
    )

    with tab_intro:
        st.subheader("Dataset overview")
        st.markdown(
            """
This dashboard presents insights from **Online Retail–style** transactions (`retail_sales.csv`).

- Each row is an invoice line (product × quantity × unit price).
- Segmentation by **Country** and **CustomerID** is supported.

**Handled data-quality issues**: missing customers, bad dates, non-positive quantities/prices.
"""
        )

        try:
            info = load_dataset_overview(db_path)
        except Exception as e:  # noqa: BLE001
            st.error(f"Could not load staging summary: {e}")
            return

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Clean rows (staging)", f"{info['rows_count']:,}")
        c2.metric("Unique customers", f"{info['customers']:,}")
        c3.metric("Countries", f"{info['countries']:,}")
        c4.metric("Date range", f"{info['min_date']} → {info['max_date']}")

        st.subheader("Monitoring status")
        with meta_connect(db_path) as conn:
            source_state = get_source_state(conn)
            last_success = get_last_success(conn)
            alerts = list_active_alerts(conn)

        m1, m2, m3 = st.columns(3)
        m1.metric("Last successful refresh", last_success["finished_at"] if last_success else "—")
        m2.metric("Last mode", last_success["mode"] if last_success else "—")
        m3.metric("Active alerts", str(len(alerts)))

        st.caption(f"DB: `{db_path}` · Raw default: `{settings.raw_csv_default}`")

        if source_state:
            sha = source_state.get("sha256") or "—"
            st.caption(
                f"Fingerprint: `{sha[:16]}…` | size {source_state.get('size_bytes')} B | "
                f"updated {source_state.get('updated_at')}"
            )

        if alerts:
            st.error("Active alerts")
            st.dataframe(alerts, use_container_width=True)

        if st.button("Check now", type="primary"):
            if not dataset.strip():
                st.warning("Enter the Kaggle dataset slug in the sidebar (owner/dataset).")
            else:
                try:
                    result = check_for_update(
                        dataset=dataset.strip(),
                        filename=filename.strip(),
                        allow_incremental=allow_incremental,
                        db_path=db_path,
                        download_first=download_from_kaggle,
                    )
                    st.success(f"Result: `{result}`")
                except Exception as e:  # noqa: BLE001
                    st.error(str(e))
                    with st.expander("Traceback"):
                        st.code(traceback.format_exc())
                st.cache_data.clear()
                st.rerun()

        st.subheader("Pipeline")
        st.markdown(
            """
**Extract** → CSV / Kaggle · **Transform** → Pandas cleaning · **Load** → SQLite staging + marts · **Serve** → this app.
"""
        )

    with tab_overview:
        total_revenue = float(monthly["revenue"].sum())
        total_units = float(monthly["units"].sum())
        total_invoices = float(monthly["invoices"].sum())

        col1, col2, col3 = st.columns(3)
        col1.metric("Total revenue", f"{total_revenue:,.0f}")
        col2.metric("Total units sold", f"{total_units:,.0f}")
        col3.metric("Total invoices", f"{total_invoices:,.0f}")

        st.subheader("Monthly revenue")
        fig = px.line(monthly, x="year_month", y="revenue", title="Monthly revenue over time")
        fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown(
            "**Takeaway:** seasonality and Q4 strength — plan inventory and campaigns around peaks."
        )

    with tab_products:
        st.subheader("Top products by revenue")
        top_n = st.slider("Top N products", min_value=5, max_value=30, value=10, step=5, key="p")
        top_products = products.head(top_n)
        fig = px.bar(
            top_products,
            x="Description",
            y="revenue",
            title=f"Top {top_n} products",
        )
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_products, use_container_width=True, hide_index=True)
        st.markdown("**Takeaway:** hero SKUs drive a large share of revenue — prioritize stock and placement.")

    with tab_customers:
        st.subheader("Top customers by revenue")
        top_n_c = st.slider("Top N customers", min_value=5, max_value=30, value=10, step=5, key="c")
        top_customers = customers.head(top_n_c)
        fig = px.bar(top_customers, x="CustomerID", y="revenue", title=f"Top {top_n_c} customers")
        fig.update_layout(template="plotly_white", height=450)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_customers, use_container_width=True, hide_index=True)
        st.markdown("**Takeaway:** revenue concentration — protect and grow key accounts.")

    with tab_countries:
        st.subheader("Top countries by revenue")
        top_n_co = st.slider("Top N countries", min_value=5, max_value=30, value=10, step=5, key="co")
        top_countries = countries.head(top_n_co)
        fig = px.bar(top_countries, x="Country", y="revenue", title=f"Top {top_n_co} countries")
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_countries, use_container_width=True, hide_index=True)
        st.markdown("**Takeaway:** UK dominance vs international upside.")


if __name__ == "__main__":
    main()
