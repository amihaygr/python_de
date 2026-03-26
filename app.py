"""Streamlit retail analytics dashboard — English UI, executive & technical modes."""

from __future__ import annotations

import inspect
import os
import traceback
from pathlib import Path

import pandas as pd
import plotly.express as px
import sqlite3
import streamlit as st

from retail_etl.analytics import RetailAnalytics
from retail_etl.db_security import assert_read_table
from retail_etl.meta import connect as meta_connect, get_last_success, get_source_state, list_active_alerts
from retail_etl.monitor import check_for_update
from retail_etl.presentation import APP_STYLE, project_root_from_app, render_architecture_presentation
from retail_etl.settings import Settings
from retail_etl.utils import configure_logging


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
        page_title="Retail Analytics | Executive & Technical Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(APP_STYLE, unsafe_allow_html=True)

    settings = Settings.load()
    db_path = settings.db_path
    root = project_root_from_app(Path(__file__))

    st.title("Retail sales intelligence")
    st.caption(
        "End-to-end pipeline: curated transactions in SQLite, interactive KPIs, and optional Kaggle refresh."
    )

    if not db_path.exists():
        st.error(
            f"SQLite database not found at `{db_path}`. "
            "Run `python -m retail_etl.cli run-all` or set `RETAIL_ETL_DB_PATH`."
        )
        return

    st.sidebar.header("Session")
    view_mode = st.sidebar.radio(
        "Presentation mode",
        options=["Executive", "Technical"],
        index=0,
        help=(
            "**Executive:** outcomes-first copy, lean technical detail. "
            "**Technical:** expanded implementation notes and source code where relevant."
        ),
    )
    executive = view_mode == "Executive"

    st.sidebar.markdown("---")
    st.sidebar.subheader("Data refresh (optional)")
    dataset = st.sidebar.text_input(
        "Kaggle dataset slug",
        value=_default_kaggle_dataset(),
        placeholder="owner/dataset-name",
    )
    filename = st.sidebar.text_input("File name inside dataset", value=_default_kaggle_filename())
    allow_incremental = st.sidebar.checkbox("Allow incremental load when safe", value=True)
    download_from_kaggle = st.sidebar.checkbox(
        "Download from Kaggle before fingerprint check",
        value=False,
        help="Requires Kaggle credentials. Overwrites the local file under data/raw.",
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

    analytics = RetailAnalytics(db_path)

    @st.cache_data(ttl=60)
    def get_kpis() -> dict[str, float | str]:
        return analytics.get_kpis()

    @st.cache_data(ttl=3600)
    def get_weekday_revenue() -> pd.DataFrame:
        return analytics.get_revenue_by_weekday()

    @st.cache_data(ttl=3600)
    def get_invoice_distribution() -> pd.DataFrame:
        return analytics.get_invoice_revenue_distribution()

    @st.cache_data(ttl=3600)
    def get_rfm_df() -> pd.DataFrame:
        return analytics.get_rfm(q=5)

    (
        tab_intro,
        tab_arch,
        tab_overview,
        tab_products,
        tab_customers,
        tab_countries,
        tab_deep,
        tab_table,
        tab_explain,
    ) = st.tabs(
        [
            "Overview",
            "Architecture",
            "KPIs & trends",
            "Products",
            "Customers",
            "Countries",
            "RFM & analytics class",
            "Staging table",
            "Project summary",
        ]
    )

    with tab_intro:
        st.subheader("Dataset scope")
        if executive:
            st.markdown(
                """
**What you are seeing:** cleaned online-retail style line items from `retail_sales.csv`, aggregated into
trusted **mart** tables for leadership-ready charts.

**Why it matters:** one governed path from raw file → validated staging → metrics you can defend in review.
"""
            )
        else:
            st.markdown(
                """
Each **row** is an invoice line (SKU × quantity × unit price). Grain supports **customer** and **country** cuts.

**Cleaning applied:** invalid dates, non-positive quantity/price filters, optional drop of rows without `CustomerID`
(configured in `CleanConfig` / `etl.py`).
"""
            )

        try:
            info = load_dataset_overview(db_path)
        except Exception as e:  # noqa: BLE001
            st.error(f"Could not load staging summary: {e}")
            return

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Clean rows (staging)", f"{info['rows_count']:,}")
        c2.metric("Distinct customers", f"{info['customers']:,}")
        c3.metric("Countries", f"{info['countries']:,}")
        c4.metric("Date range", f"{info['min_date']} → {info['max_date']}")

        st.subheader("Operational status")
        with meta_connect(db_path) as conn:
            source_state = get_source_state(conn)
            last_success = get_last_success(conn)
            alerts = list_active_alerts(conn)

        m1, m2, m3 = st.columns(3)
        m1.metric("Last successful refresh", last_success["finished_at"] if last_success else "—")
        m2.metric("Last run mode", last_success["mode"] if last_success else "—")
        m3.metric("Active alerts", str(len(alerts)))

        if not executive:
            st.caption(f"Database: `{db_path}` · Default raw CSV: `{settings.raw_csv_default}`")

        if source_state:
            sha = source_state.get("sha256") or "—"
            st.caption(
                f"Source fingerprint: `{sha[:16]}…` · {source_state.get('size_bytes')} bytes · "
                f"updated {source_state.get('updated_at')}"
            )

        if alerts:
            st.error("Active data-quality or pipeline alerts")
            st.dataframe(alerts, use_container_width=True)

        if st.button("Run refresh check", type="primary"):
            if not dataset.strip():
                st.warning("Enter a Kaggle dataset slug in the sidebar (format: owner/dataset).")
            else:
                try:
                    result = check_for_update(
                        dataset=dataset.strip(),
                        filename=filename.strip(),
                        allow_incremental=allow_incremental,
                        db_path=db_path,
                        download_first=download_from_kaggle,
                    )
                    st.success(f"Monitor result: `{result}`")
                except Exception as e:  # noqa: BLE001
                    st.error(str(e))
                    with st.expander("Traceback"):
                        st.code(traceback.format_exc())
                st.cache_data.clear()
                st.rerun()

        st.subheader("Pipeline at a glance")
        st.markdown(
            "**Extract** → CSV / Kaggle · **Transform** → Pandas · **Load** → SQLite (staging + marts) · **Serve** → this app."
        )

    with tab_arch:
        render_architecture_presentation(st, root, executive_mode=executive)

    with tab_overview:
        kpis = get_kpis()
        total_revenue = float(monthly["revenue"].sum())
        total_units = float(monthly["units"].sum())
        total_invoices = float(monthly["invoices"].sum())

        st.subheader("Headline metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total revenue", f"{total_revenue:,.0f}")
        col2.metric("Units sold", f"{total_units:,.0f}")
        col3.metric("Invoices", f"{total_invoices:,.0f}")

        col4, col5, col6 = st.columns(3)
        col4.metric("Avg invoice value", f"{kpis['avg_invoice_value']:,.2f}")
        col5.metric("Avg lines / invoice", f"{kpis['avg_lines_per_invoice']:,.2f}")
        col6.metric("Avg revenue / customer", f"{kpis['avg_spend_per_customer']:,.2f}")

        col7, col8, _ = st.columns(3)
        col7.metric("UK revenue share", f"{kpis['uk_revenue_share']*100:.1f}%")
        col8.metric("Distinct SKUs", f"{int(kpis['products']):,}")

        st.subheader("Monthly revenue")
        fig = px.line(
            monthly,
            x="year_month",
            y="revenue",
            title="Revenue by month",
            labels={"year_month": "Month", "revenue": "Revenue"},
        )
        fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(fig, use_container_width=True)
        if executive:
            st.info("**Executive read:** trend and seasonality drive inventory and campaign timing—watch peaks and troughs.")
        else:
            st.caption("Seasonality note: align stock and marketing with peaks; investigate structural breaks after refreshes.")

        st.subheader("Revenue by weekday")
        weekday = get_weekday_revenue()
        weekday_fig = px.bar(
            weekday,
            x="weekday",
            y="revenue",
            title="Revenue by weekday",
            labels={"weekday": "Weekday", "revenue": "Revenue"},
        )
        weekday_fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(weekday_fig, use_container_width=True)
        if not weekday.empty:
            best = weekday.sort_values("revenue", ascending=False).iloc[0]
            st.caption(f"Top weekday: **{best['weekday']}** → {best['revenue']:,.0f} revenue.")

        st.subheader("Invoice size distribution")
        inv = get_invoice_distribution()
        hist_fig = px.histogram(
            inv,
            x="invoice_revenue",
            nbins=40,
            title="Revenue per invoice",
            labels={"invoice_revenue": "Invoice revenue"},
        )
        hist_fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(hist_fig, use_container_width=True)
        if not inv.empty:
            q50 = float(inv["invoice_revenue"].quantile(0.5))
            q90 = float(inv["invoice_revenue"].quantile(0.9))
            q95 = float(inv["invoice_revenue"].quantile(0.95))
            st.caption(f"Median **{q50:,.2f}** · P90 **{q90:,.2f}** · P95 **{q95:,.2f}**")

    with tab_products:
        st.subheader("Top products by revenue")
        top_n = st.slider("Top N", min_value=5, max_value=30, value=10, step=5, key="p")
        top_products = products.head(top_n)
        fig = px.bar(
            top_products,
            x="Description",
            y="revenue",
            title=f"Top {top_n} products",
            labels={"Description": "Product", "revenue": "Revenue"},
        )
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)
        if not executive:
            st.dataframe(top_products, use_container_width=True, hide_index=True)
        if executive:
            st.success("**Focus:** hero SKUs typically concentrate revenue—protect availability and placement.")
        else:
            st.caption("Concentration risk: a small set of SKUs may dominate; validate with Pareto offline if needed.")

    with tab_customers:
        st.subheader("Top customers by revenue")
        top_n_c = st.slider("Top N", min_value=5, max_value=30, value=10, step=5, key="c")
        top_customers = customers.head(top_n_c)
        fig = px.bar(
            top_customers,
            x="CustomerID",
            y="revenue",
            title=f"Top {top_n_c} customers",
            labels={"CustomerID": "Customer ID", "revenue": "Revenue"},
        )
        fig.update_layout(template="plotly_white", height=450)
        st.plotly_chart(fig, use_container_width=True)
        if not executive:
            st.dataframe(top_customers, use_container_width=True, hide_index=True)
        if executive:
            st.success("**Focus:** revenue concentration—prioritise retention and growth for key accounts.")
        else:
            st.caption("Consider combining with RFM tab for lifecycle actions (retain / reactivate).")

    with tab_countries:
        st.subheader("Top countries by revenue")
        top_n_co = st.slider("Top N", min_value=5, max_value=30, value=10, step=5, key="co")
        top_countries = countries.head(top_n_co)
        fig = px.bar(
            top_countries,
            x="Country",
            y="revenue",
            title=f"Top {top_n_co} countries",
            labels={"Country": "Country", "revenue": "Revenue"},
        )
        fig.update_layout(template="plotly_white", xaxis_tickangle=-45, height=500)
        st.plotly_chart(fig, use_container_width=True)
        if not executive:
            st.dataframe(top_countries, use_container_width=True, hide_index=True)
        if executive:
            st.info("**Geography:** UK share is often dominant in this sample—test international upside hypotheses explicitly.")
        else:
            st.caption("Slice further with `mart_country_summary` exports if you add currency or cost data later.")

    with tab_deep:
        st.subheader("RFM customer segmentation")
        st.caption(
            "**R**ecency (days since last purchase), **F**requency (invoice count), **M**onetary (cumulative revenue)."
        )

        rfm_df = get_rfm_df()
        if rfm_df.empty:
            st.warning("No RFM rows available (check staging dates).")
            return

        seg_counts = (
            rfm_df.groupby("rfm_segment")
            .agg(
                customers=("CustomerID", "count"),
                avg_monetary=("monetary", "mean"),
                avg_recency=("recency_days", "mean"),
            )
            .reset_index()
            .sort_values("customers", ascending=False)
            .head(20)
        )

        seg_fig = px.bar(
            seg_counts,
            x="rfm_segment",
            y="customers",
            title="Largest RFM segments (by customer count)",
            labels={"rfm_segment": "RFM segment", "customers": "Customers"},
        )
        seg_fig.update_layout(template="plotly_white", height=420, xaxis_tickangle=-45)
        st.plotly_chart(seg_fig, use_container_width=True)

        best = rfm_df.sort_values("monetary", ascending=False).head(1).iloc[0]
        st.caption(
            f"Highest monetary customer **{int(best['CustomerID'])}** · "
            f"revenue {best['monetary']:,.2f} · invoices {int(best['frequency'])} · "
            f"recency {best['recency_days']:.1f} days."
        )

        with st.expander("RetailAnalytics class — purpose & API", expanded=not executive):
            st.markdown(
                """
The UI stays thin; quantitative logic lives in **`src/retail_etl/analytics.py`** (`RetailAnalytics`).

| Method | Use |
|--------|-----|
| `get_kpis()` | Dashboard headline KPIs |
| `get_revenue_by_weekday()` | Weekday seasonality |
| `get_invoice_revenue_distribution()` | Invoice-level histogram input |
| `get_rfm(q=5)` | Segmentation with quantile bins |
"""
            )
            st.divider()
            st.markdown("**Usage example (same pattern as this app):**")
            sample_usage = """from pathlib import Path
from retail_etl.analytics import RetailAnalytics

db_path = Path("data/db/retail.db")
analytics = RetailAnalytics(db_path)

kpis = analytics.get_kpis()
rfm_df = analytics.get_rfm(q=5)
"""
            st.code(sample_usage, language="python")

        with st.expander("`get_rfm` implementation (technical)", expanded=not executive):
            @st.cache_data(ttl=3600)
            def _cached_get_rfm_source() -> str:
                return inspect.getsource(RetailAnalytics.get_rfm)

            st.code(_cached_get_rfm_source(), language="python")

    with tab_explain:
        if executive:
            st.markdown(
                """
### Executive summary
- **Single governed path** from raw file to curated SQLite marts used by this dashboard.
- **Monitoring** records source fingerprints and can trigger controlled refreshes when data or schema drift.
- **Analytics** (`RetailAnalytics`) isolates SQL + pandas so the UI remains maintainable.

For engineering depth, switch sidebar mode to **Technical** or open the **Architecture** tab.
"""
            )
        else:
            st.markdown(
                """
### Project summary (technical)

**1. ETL** — `data/raw/retail_sales.csv` → `clean_sales` → `stg_sales_clean` → mart tables (`mart_*`).

**2. Monitoring & Kaggle** — `ingest_kaggle.py` downloads; `monitor.py` compares fingerprints and schema vs
`EXPECTED_RAW_COLUMNS`; alerts in `meta_*` tables.

**3. SQL hygiene** — Dynamic reads use **allowlisted** table names (`db_security.py`). Query text lives under
`src/retail_etl/sql/*.sql` and is loaded via `sql_loader.py`.

**4. Quality gates** — `tests/` with `pytest`; `Dockerfile` for repeatable runs on port **8501**.

**5. Deep dive** — Full folder/file map: **Architecture** tab.
"""
            )

    with tab_table:
        st.subheader("Central staging table: `stg_sales_clean`")
        st.caption("Built after transform; all marts are derived from this table.")

        st.markdown(
            """
**Role:** cleaned grain at invoice-line level before aggregation.

**Key columns**

| Column | Meaning |
|--------|---------|
| `InvoiceNo` | Invoice identifier |
| `StockCode` | SKU |
| `Description` | Product text (trimmed) |
| `Quantity` | Units sold |
| `InvoiceDate` | Normalised timestamp string for SQLite |
| `UnitPrice` | Unit price |
| `CustomerID` | Customer key |
| `Country` | Country |
| `line_total` | `Quantity * UnitPrice` |

**Cleaning rules (summary)**  
Parse dates; coerce numerics; optional drop missing `CustomerID`; filter `Quantity >= 1` and `UnitPrice > 0`
(returns / invalid lines excluded per project defaults).
"""
        )


if __name__ == "__main__":
    main()
