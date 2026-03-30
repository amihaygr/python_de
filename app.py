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
from retail_etl.local_time import format_utc_iso_as_israel, localize_alert_rows
from retail_etl.meta import clear_alerts, connect as meta_connect, get_last_success, get_source_state, list_active_alerts
from retail_etl.monitor import check_for_update
from retail_etl.presentation import APP_STYLE, project_root_from_app, render_architecture_presentation
from retail_etl.sql_loader import load_sql
from retail_etl.settings import DEFAULT_RETAIL_KAGGLE_DATASET, DEFAULT_RETAIL_KAGGLE_FILENAME, Settings
from retail_etl.utils import configure_logging

_KAGGLE_PLACEHOLDER_SLUGS = frozenset({"owner/dataset-name"})


def _default_kaggle_dataset() -> str:
    try:
        sec = st.secrets
        if hasattr(sec, "get"):
            v = sec.get("RETAIL_KAGGLE_DATASET", "")
            if v:
                return str(v)
    except Exception:
        pass
    v = os.environ.get("RETAIL_KAGGLE_DATASET", "").strip()
    if v:
        return v
    return DEFAULT_RETAIL_KAGGLE_DATASET


def _default_kaggle_filename() -> str:
    try:
        sec = st.secrets
        if hasattr(sec, "get"):
            v = sec.get("RETAIL_KAGGLE_FILENAME", "")
            if v:
                return str(v)
    except Exception:
        pass
    v = os.environ.get("RETAIL_KAGGLE_FILENAME", "").strip()
    if v:
        return v
    return DEFAULT_RETAIL_KAGGLE_FILENAME


def get_connection(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=30.0)


@st.cache_data(ttl=60)
def load_dataset_overview(db_path: Path) -> dict:
    with get_connection(db_path) as conn:
        row = conn.execute(load_sql("app_dataset_overview.sql")).fetchone()
    return {
        "rows_count": int(row[0]),
        "customers": int(row[1]),
        "countries": int(row[2]),
        "min_date": row[3],
        "max_date": row[4],
    }


@st.cache_data(ttl=60)
def load_staging_for_slicers(db_path: Path) -> pd.DataFrame:
    with get_connection(db_path) as conn:
        df = pd.read_sql_query(load_sql("app_staging_for_slicers.sql"), conn)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce").fillna(0.0)
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0.0)
    df["CustomerID"] = pd.to_numeric(df["CustomerID"], errors="coerce")
    df["Country"] = df["Country"].fillna("").astype(str).str.strip()
    df["Description"] = df["Description"].fillna("").astype(str).str.strip()
    return df[df["InvoiceDate"].notna()].copy()


def compute_rfm_from_frame(df: pd.DataFrame, q: int = 5) -> pd.DataFrame:
    required = ["CustomerID", "InvoiceNo", "InvoiceDate", "line_total"]
    if not set(required).issubset(df.columns):
        return pd.DataFrame()
    work = df[required].copy()
    work["InvoiceDate"] = pd.to_datetime(work["InvoiceDate"], errors="coerce")
    work["line_total"] = pd.to_numeric(work["line_total"], errors="coerce").fillna(0.0)
    work["CustomerID"] = pd.to_numeric(work["CustomerID"], errors="coerce")
    work = work.dropna(subset=["InvoiceDate", "CustomerID"])
    if work.empty:
        return pd.DataFrame()

    max_dt = work["InvoiceDate"].max()
    agg = (
        work.groupby("CustomerID", as_index=False)
        .agg(last_invoice=("InvoiceDate", "max"), frequency=("InvoiceNo", "nunique"), monetary=("line_total", "sum"))
    )
    agg["recency_days"] = (max_dt - agg["last_invoice"]).dt.total_seconds() / 86400.0

    def _qcut_codes(values: pd.Series) -> tuple[int, pd.Series]:
        try:
            codes = pd.qcut(values, q=q, labels=False, duplicates="drop")
        except ValueError:
            codes = pd.Series([0] * len(values), index=values.index, dtype="int64")
        codes = codes.fillna(0).astype(int)
        n_bins = int(codes.max()) + 1 if len(codes) else 1
        return max(n_bins, 1), codes

    r_bins, r_codes = _qcut_codes(agg["recency_days"].fillna(0))
    f_bins, f_codes = _qcut_codes(agg["frequency"].fillna(0))
    m_bins, m_codes = _qcut_codes(agg["monetary"].fillna(0.0))
    agg["r_score"] = (r_bins - r_codes).clip(lower=1).astype(int)
    agg["f_score"] = (f_codes + 1).astype(int)
    agg["m_score"] = (m_codes + 1).astype(int)
    agg["rfm_segment"] = agg["r_score"].astype(str) + agg["f_score"].astype(str) + agg["m_score"].astype(str)
    return agg


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
        placeholder="owner/dataset from kaggle.com/datasets/…",
        help=(
            "Copy from the dataset URL after `/datasets/`. "
            f"Project default: `{DEFAULT_RETAIL_KAGGLE_DATASET}`."
        ),
    )
    if dataset.strip().lower() in {s.lower() for s in _KAGGLE_PLACEHOLDER_SLUGS}:
        st.sidebar.warning(
            "`owner/dataset-name` is only an example. Paste the real slug from Kaggle "
            f"(this project defaults to `{DEFAULT_RETAIL_KAGGLE_DATASET}`)."
        )
    filename = st.sidebar.text_input("File name inside dataset", value=_default_kaggle_filename())
    allow_incremental = st.sidebar.checkbox("Allow incremental load when safe", value=True)
    download_from_kaggle = st.sidebar.checkbox(
        "Download from Kaggle before fingerprint check",
        value=False,
        help=(
            "Requires Kaggle credentials. If the raw CSV is missing, a download is attempted automatically; "
            "check this box to overwrite an existing file under data/raw."
        ),
    )

    with st.spinner("Loading staging table…"):
        try:
            staging_df = load_staging_for_slicers(db_path)
        except Exception as e:  # noqa: BLE001
            st.error(f"Failed to load staging table for slicers: {e}")
            return
    if staging_df.empty:
        st.error("Staging table is empty; run refresh first.")
        return

    st.sidebar.markdown("---")
    st.sidebar.subheader("Advanced slicers")
    min_dt = staging_df["InvoiceDate"].min().date()
    max_dt = staging_df["InvoiceDate"].max().date()
    date_range = st.sidebar.date_input(
        "Invoice date range",
        value=(min_dt, max_dt),
        min_value=min_dt,
        max_value=max_dt,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        from_date, to_date = date_range
    else:
        from_date, to_date = min_dt, max_dt

    country_options = sorted([c for c in staging_df["Country"].dropna().unique().tolist() if c])
    selected_countries = st.sidebar.multiselect(
        "Countries",
        options=country_options,
        default=country_options,
    )
    max_line_total = float(staging_df["line_total"].quantile(0.99)) if not staging_df.empty else 0.0
    min_line_total = st.sidebar.slider(
        "Minimum line total",
        min_value=0.0,
        max_value=max(max_line_total, 1.0),
        value=0.0,
        step=1.0,
    )
    product_search = st.sidebar.text_input("Product contains", value="").strip().lower()

    filtered_df = staging_df[
        (staging_df["InvoiceDate"].dt.date >= from_date)
        & (staging_df["InvoiceDate"].dt.date <= to_date)
        & (staging_df["line_total"] >= min_line_total)
    ].copy()
    if selected_countries:
        filtered_df = filtered_df[filtered_df["Country"].isin(selected_countries)]
    else:
        filtered_df = filtered_df.iloc[0:0]
    if product_search:
        filtered_df = filtered_df[filtered_df["Description"].str.lower().str.contains(product_search, na=False)]

    if filtered_df.empty:
        st.warning("No rows match the current slicers. Widen the filters in the sidebar.")
        return

    filtered_df["year_month"] = filtered_df["InvoiceDate"].dt.to_period("M").astype(str)
    filtered_df["weekday"] = filtered_df["InvoiceDate"].dt.day_name().str.slice(0, 3)
    invoice_totals = (
        filtered_df.groupby("InvoiceNo", as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "invoice_revenue"})
    )
    monthly_filtered = (
        filtered_df.groupby("year_month", as_index=False)
        .agg(revenue=("line_total", "sum"), units=("Quantity", "sum"), invoices=("InvoiceNo", "nunique"))
        .sort_values("year_month")
    )
    weekday_order = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    weekday_filtered = (
        filtered_df.groupby("weekday", as_index=False)
        .agg(revenue=("line_total", "sum"))
        .assign(weekday_num=lambda d: d["weekday"].map({w: i for i, w in enumerate(weekday_order)}))
        .sort_values("weekday_num")
    )
    products_filtered = (
        filtered_df.groupby("Description", as_index=False)
        .agg(revenue=("line_total", "sum"), units=("Quantity", "sum"), invoices=("InvoiceNo", "nunique"))
        .sort_values("revenue", ascending=False)
    )
    customers_filtered = (
        filtered_df.groupby("CustomerID", as_index=False)
        .agg(revenue=("line_total", "sum"), units=("Quantity", "sum"), invoices=("InvoiceNo", "nunique"))
        .sort_values("revenue", ascending=False)
    )
    countries_filtered = (
        filtered_df.groupby("Country", as_index=False)
        .agg(revenue=("line_total", "sum"), units=("Quantity", "sum"), invoices=("InvoiceNo", "nunique"))
        .sort_values("revenue", ascending=False)
    )
    filtered_kpis = {
        "revenue": float(filtered_df["line_total"].sum()),
        "units": float(filtered_df["Quantity"].sum()),
        "invoices": float(filtered_df["InvoiceNo"].nunique()),
        "customers": float(filtered_df["CustomerID"].nunique()),
        "products": float(filtered_df["StockCode"].nunique()),
        "avg_invoice_value": float(invoice_totals["invoice_revenue"].mean() if not invoice_totals.empty else 0.0),
        "avg_lines_per_invoice": float(len(filtered_df) / max(filtered_df["InvoiceNo"].nunique(), 1)),
        "avg_spend_per_customer": float(
            filtered_df["line_total"].sum() / max(filtered_df["CustomerID"].nunique(), 1)
        ),
        "uk_revenue_share": float(
            filtered_df.loc[filtered_df["Country"].eq("United Kingdom"), "line_total"].sum()
            / max(filtered_df["line_total"].sum(), 1.0)
        ),
    }

    analytics = RetailAnalytics(db_path)

    (
        tab_intro,
        tab_overview,
        tab_products,
        tab_customers,
        tab_countries,
        tab_deep,
        tab_arch,
        tab_explain,
        tab_table,
    ) = st.tabs(
        [
            "Overview",
            "KPIs & trends",
            "Products",
            "Customers",
            "Countries",
            "RFM & analytics class",
            "Architecture",
            "Project summary",
            "Staging table",
        ]
    )

    with tab_intro:
        st.subheader("Dataset scope")
        if executive:
            st.markdown(
                """
**What you are seeing:** cleaned online-retail style line items from the raw CSV, aggregated into
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
        st.caption(
            f"Slicer result: **{len(filtered_df):,}** rows · "
            f"{filtered_df['InvoiceNo'].nunique():,} invoices · "
            f"{filtered_df['CustomerID'].nunique():,} customers."
        )

        st.subheader("Operational status")
        with meta_connect(db_path) as conn:
            source_state = get_source_state(conn)
            last_success = get_last_success(conn)
            alerts = list_active_alerts(conn)

        m1, m2, m3 = st.columns(3)
        m1.metric(
            "Last successful refresh",
            format_utc_iso_as_israel(last_success["finished_at"]) if last_success else "—",
            help="Stored in UTC; shown in Asia/Jerusalem (IST/IDT).",
        )
        m2.metric("Last run mode", last_success["mode"] if last_success else "—")
        m3.metric("Active alerts", str(len(alerts)))

        if not executive:
            st.caption(f"Database: `{db_path}` · Default raw CSV: `{settings.raw_csv_default}`")

        if source_state:
            sha = source_state.get("sha256") or "—"
            st.caption(
                f"Source fingerprint: `{sha[:16]}…` · {source_state.get('size_bytes')} bytes · "
                f"last check time (Israel): **{format_utc_iso_as_israel(source_state.get('updated_at'))}**"
            )

        if alerts:
            st.error("Active data-quality or pipeline alerts")
            st.dataframe(
                pd.DataFrame(localize_alert_rows(alerts)),
                use_container_width=True,
            )
            if any(a.get("kind") == "etl_failure" for a in alerts):
                if st.button("Clear ETL failure alerts (keep schema alerts)"):
                    with meta_connect(db_path) as conn:
                        clear_alerts(conn, kind="etl_failure")
                    st.cache_data.clear()
                    st.rerun()

        if st.button("Run refresh check", type="primary"):
            slug = dataset.strip()
            if not slug:
                st.warning("Enter a Kaggle dataset slug in the sidebar (format: owner/dataset).")
            elif slug.lower() in {s.lower() for s in _KAGGLE_PLACEHOLDER_SLUGS}:
                st.warning(
                    "Replace the example slug with a real dataset from Kaggle "
                    f"(e.g. `{DEFAULT_RETAIL_KAGGLE_DATASET}`)."
                )
            else:
                try:
                    result = check_for_update(
                        dataset=slug,
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
        total_revenue = float(filtered_kpis["revenue"])
        total_units = float(filtered_kpis["units"])
        total_invoices = float(filtered_kpis["invoices"])

        st.subheader("Headline metrics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total revenue", f"{total_revenue:,.0f}")
        col2.metric("Units sold", f"{total_units:,.0f}")
        col3.metric("Invoices", f"{total_invoices:,.0f}")

        col4, col5, col6 = st.columns(3)
        col4.metric("Avg invoice value", f"{filtered_kpis['avg_invoice_value']:,.2f}")
        col5.metric("Avg lines / invoice", f"{filtered_kpis['avg_lines_per_invoice']:,.2f}")
        col6.metric("Avg revenue / customer", f"{filtered_kpis['avg_spend_per_customer']:,.2f}")

        col7, col8, _ = st.columns(3)
        col7.metric("UK revenue share", f"{filtered_kpis['uk_revenue_share']*100:.1f}%")
        col8.metric("Distinct SKUs", f"{int(filtered_kpis['products']):,}")

        st.subheader("Monthly revenue")
        fig = px.line(
            monthly_filtered,
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
        weekday_fig = px.bar(
            weekday_filtered,
            x="weekday",
            y="revenue",
            title="Revenue by weekday",
            labels={"weekday": "Weekday", "revenue": "Revenue"},
        )
        weekday_fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(weekday_fig, use_container_width=True)
        if not weekday_filtered.empty:
            best = weekday_filtered.sort_values("revenue", ascending=False).iloc[0]
            st.caption(f"Top weekday: **{best['weekday']}** → {best['revenue']:,.0f} revenue.")

        st.subheader("Invoice size distribution")
        hist_fig = px.histogram(
            invoice_totals,
            x="invoice_revenue",
            nbins=40,
            title="Revenue per invoice",
            labels={"invoice_revenue": "Invoice revenue"},
        )
        hist_fig.update_layout(template="plotly_white", height=420)
        st.plotly_chart(hist_fig, use_container_width=True)
        if not invoice_totals.empty:
            q50 = float(invoice_totals["invoice_revenue"].quantile(0.5))
            q90 = float(invoice_totals["invoice_revenue"].quantile(0.9))
            q95 = float(invoice_totals["invoice_revenue"].quantile(0.95))
            st.caption(f"Median **{q50:,.2f}** · P90 **{q90:,.2f}** · P95 **{q95:,.2f}**")

    with tab_products:
        st.subheader("Top products by revenue")
        top_n = st.slider("Top N", min_value=5, max_value=30, value=10, step=5, key="p")
        top_products = products_filtered.head(top_n)
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
        top_customers = customers_filtered.head(top_n_c)
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
        top_countries = countries_filtered.head(top_n_co)
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

        rfm_df = compute_rfm_from_frame(filtered_df, q=5)
        if rfm_df.empty:
            st.warning("No RFM rows available (check staging dates).")
            return

        c_r1, c_r2, c_r3 = st.columns(3)
        max_recency = float(max(rfm_df["recency_days"].max(), 1.0))
        recency_max = c_r1.slider(
            "Max recency (days)",
            min_value=0.0,
            max_value=max_recency,
            value=max_recency,
            step=1.0,
            key="rfm_recency_max",
        )
        min_frequency = c_r2.slider(
            "Min frequency (invoices)",
            min_value=1,
            max_value=int(max(rfm_df["frequency"].max(), 1)),
            value=1,
            step=1,
            key="rfm_freq_min",
        )
        min_monetary = c_r3.slider(
            "Min monetary",
            min_value=0.0,
            max_value=float(max(rfm_df["monetary"].quantile(0.99), 1.0)),
            value=0.0,
            step=10.0,
            key="rfm_monetary_min",
        )
        seg_options = sorted(rfm_df["rfm_segment"].dropna().astype(str).unique().tolist())
        seg_selected = st.multiselect(
            "RFM segments",
            options=seg_options,
            default=seg_options[: min(15, len(seg_options))] if len(seg_options) > 15 else seg_options,
            key="rfm_segments",
        )
        rfm_df = rfm_df[
            (rfm_df["recency_days"] <= recency_max)
            & (rfm_df["frequency"] >= min_frequency)
            & (rfm_df["monetary"] >= min_monetary)
        ].copy()
        if seg_selected:
            rfm_df = rfm_df[rfm_df["rfm_segment"].isin(seg_selected)]
        if rfm_df.empty:
            st.warning("No customers match current RFM slicers. Relax the RFM filters.")
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

**1. ETL** — `data/raw/` CSV (default `online_retail.csv`) → `clean_sales` → `stg_sales_clean` → mart tables (`mart_*`).

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
        st.caption("Preview below respects sidebar slicers.")

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
        st.dataframe(
            filtered_df[
                [
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
            ].head(500),
            use_container_width=True,
            hide_index=True,
        )


if __name__ == "__main__":
    main()
