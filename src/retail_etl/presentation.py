"""Dashboard presentation copy and architecture walkthrough (English, LTR)."""

from __future__ import annotations

from pathlib import Path

import plotly.graph_objects as go

# Subtle, presentation-friendly styling (LTR, English UI)
APP_STYLE = """
<style>
    .main .block-container {
        max-width: 1200px;
        padding-top: 0.75rem;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }
    h1 { font-weight: 600; letter-spacing: -0.02em; }
    h2, h3 { font-weight: 600; }
    [data-testid="stSidebar"] {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, #fafafa 0%, #ffffff 100%);
        border: 1px solid #eaeaea;
        border-radius: 8px;
        padding: 0.5rem 0.75rem;
    }
    button[data-testid="baseButton-primary"] { font-weight: 600; }
    [data-testid="stVerticalBlock"] > div:has([data-testid="stPlotlyChart"]) {
        border-radius: 10px;
    }
</style>
"""

# English presenter “what to say” blocks — shown when “Show presenter hints” is enabled in the sidebar.
# A detailed Hebrew walkthrough for the same flow lives in docs/presentation_personal_guide_he.md.
PRESENTER_HINTS: dict[str, str] = {
    "intro": """
**Goal:** Show the system is *governed* — not only charts, but source, runs, and alerts.

**Walk through in order**
1. **Four headline metrics** — scope after cleaning (rows, customers, countries, date range). Say: “This is the grain every downstream chart uses.”
2. **“Slicer result” line** — how many rows remain after sidebar filters. Say: “Everything from here on respects the same filters — no ETL rerun.”
3. **Operational status** — last successful refresh, run mode, alert count. Say: “A successful check is recorded even when data did not change — so the timestamp updates.”
4. **Fingerprint / SHA** — Say: “You can defend auditability against the raw file.”
5. **Run refresh check** — Say: “This triggers monitoring against Kaggle/CSV: fingerprint, schema, controlled load.”
6. **Sankey diagram (below)** — Say: “After staging, **marts** (`mart_*`) hold **business aggregates** (revenue by month/product/etc.); **meta** (`meta_*`) holds **pipeline observability** (fingerprints, runs, alerts). The dashboard uses both: charts from analytics/staging, status from meta.”
""",
    "kpis": """
**Goal:** Trends, seasonality, and distributions — *in the filtered context*.

**In screen order**
1. **Headline metrics** — Total revenue / Units / Invoices, then averages. Say: “Direct business KPIs from filtered staging.”
2. **Monthly revenue** — Area + 3-month rolling trend. Say: “Seasonality and any break when data was refreshed.”
3. **Revenue by weekday** — Say: “Strong days for campaigns and operations.”
4. **Shopping rhythm (heatmap)** — Say: “Weekday × hour: absolute mode = money; % mode = *intraday shape* even when totals differ; business-hours zooms the x-axis.”
5. **Invoice size distribution** — Histogram + box. Say: “Tail risk for large invoices — relevant for discounts and credit policy.”
""",
    "products": """
**Goal:** Product concentration (Pareto).

**What to say**
- “Horizontal bar chart of Top N products by revenue.” Change **Top N** to show interactivity.
- “If a few SKUs dominate revenue — stock dependency; a flatter mix means a broader portfolio.”
- In **Technical** mode: “The table below shows the exact figures.”
""",
    "customers": """
**Goal:** Customer concentration and risk.

**What to say**
- “Same global slicers — comparison with Products is apples-to-apples.”
- “Top accounts deserve retention focus; call out a single customer with outsized share.”
- “Link to the RFM tab for lifecycle actions.”
""",
    "countries": """
**Goal:** Geography and revenue share.

**What to say**
- **Treemap** — “Tile size ∝ revenue; color reinforces differences.”
- “In this classic dataset the UK is often dominant — discuss international upside.”
- Adjust **Top N** to show geographic depth.
""",
    "rfm": """
**Goal:** Recency / Frequency / Monetary segmentation — *on filtered data*.

**What to say**
1. **Sliders** — “Narrow to relevant customers: not too stale, not one-off, not negligible spend.”
2. **Segment bar chart** — “Which RFM codes best represent the base.”
3. **Bubble chart** — “X = recency (days), Y = frequency, bubble size = monetary.”
4. “Logic stays in `RetailAnalytics` / helpers — the UI stays thin.”
""",
    "arch": """
**Goal:** Engineering transparency — layout, external SQL, tests.

**What to say**
- “Separate UI from logic: `app.py` renders, `retail_etl` implements.”
- “Dynamic queries use an allowlist; SQL lives in files for review.”
- Open one expander as an example: “How we change SQL without hunting strings in Python.”
""",
    "summary": """
**Goal:** Quick close — value proposition.

**What to say**
- **Executive:** “One governed path from raw file to board-ready metrics.”
- **Technical:** “Docker, pytest, metadata tables, CLI — course/PoC ready and extensible.”
""",
    "staging": """
**Goal:** Prove grain and data contracts.

**What to say**
- “Each row is an invoice line; `line_total` is an engineered feature from ETL.”
- “Preview respects slicers — same logic as the charts.”
- “For cleaning rules, point to `CleanConfig` and quantity/price filters.”
""",
}


def render_presenter_hint(st, tab_key: str, *, enabled: bool) -> None:
    """Expandable English presenter script (see PRESENTER_HINTS)."""
    if not enabled:
        return
    body = PRESENTER_HINTS.get(tab_key)
    if not body:
        return
    with st.expander("Presenter hints — what to say (English)", expanded=False):
        st.markdown(body)


def render_pipeline_sankey(st, *, colorway: list[str] | None = None) -> None:
    """Interactive pipeline diagram (same logical story as the Mermaid diagram in the presentation doc)."""
    # Light node fills so default label text stays dark-on-light and highly readable (not grey-on-grey flows).
    colors = colorway or [
        "#BFDBFE",
        "#BAE6FD",
        "#99F6E4",
        "#BBF7D0",
        "#FDE68A",
        "#DDD6FE",
        "#CBD5E1",
    ]
    labels = [
        "① Kaggle / local CSV",
        "② Ingest + fingerprint",
        "③ clean_sales (Pandas)",
        "④ stg_sales_clean",
        "⑤ Mart tables",
        "⑥ Meta (runs / alerts)",
        "⑦ Streamlit + Plotly",
    ]
    # Logical flow: linear ETL + parallel observability into the dashboard
    source = [0, 1, 2, 3, 3, 4, 5]
    target = [1, 2, 3, 4, 5, 6, 6]
    value = [10, 10, 10, 10, 10, 10, 10]
    n_links = len(source)
    link_colors = ["rgba(100, 116, 139, 0.22)"] * n_links
    fig = go.Figure(
        data=[
            go.Sankey(
                arrangement="snap",
                valueformat=".0f",
                textfont=dict(
                    family="Segoe UI, system-ui, sans-serif",
                    size=15,
                    color="#0f172a",
                ),
                node=dict(
                    pad=28,
                    thickness=36,
                    line=dict(color="#64748B", width=1),
                    label=labels,
                    color=colors[: len(labels)],
                    hovertemplate="%{label}<extra></extra>",
                ),
                link=dict(
                    source=source,
                    target=target,
                    value=value,
                    color=link_colors,
                ),
            )
        ]
    )
    fig.update_layout(
        title=dict(
            text="Data pipeline — logical flow (same story as the presentation guide)",
            font=dict(size=17, color="#0f172a"),
            x=0.02,
            xanchor="left",
        ),
        height=500,
        margin=dict(l=24, r=24, t=64, b=32),
        font=dict(family="Segoe UI, system-ui, sans-serif", size=14, color="#0f172a"),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
    )
    st.plotly_chart(fig, width="stretch")
    st.caption(
        "Flows are **even widths** (illustrative). After staging, the fork is: **Marts** = aggregated business facts "
        "(`mart_*`, e.g. revenue by month/product/customer/country). **Meta** = pipeline observability "
        "(`meta_*`: fingerprints, runs, alerts). Both are written in SQLite; the dashboard uses marts/analytics for "
        "charts and meta for operational status."
    )


def project_root_from_app(app_file: Path) -> Path:
    """Project root when given the path to app.py."""
    return app_file.resolve().parent


def render_architecture_presentation(st, root: Path, *, executive_mode: bool) -> None:
    """Step-by-step architecture: folders, modules, optional deep SQL."""
    st.markdown(
        """
### Solution architecture
Use expanders to control depth: **high-level for leadership**, **full detail for engineers**.
""",
        unsafe_allow_html=False,
    )

    detail_open = not executive_mode

    steps = [
        (
            "Step 1 — Repository layout",
            """
**Root** contains the Streamlit entrypoint, Python package under `src/`, tests, local data, and container assets.

| Area | Role |
|------|------|
| `app.py` | Streamlit UI only (charts, narrative, monitoring actions) |
| `src/retail_etl/` | ETL, metadata, monitoring, analytics, SQL files |
| `tests/` | `pytest` unit tests |
| `data/raw/` | Source CSV (local) |
| `data/db/` | SQLite database (`retail.db`) |
| `Dockerfile` | Runnable image (default port **8501**) |
| `requirements.txt` | Runtime dependencies |
| `pyproject.toml` | Package discovery (`src` layout) |
| `.streamlit/` | Streamlit server config |
""",
        ),
        (
            "Step 2 — Data flow (E → T → L → Serve)",
            """
1. **Extract** — Local CSV and/or Kaggle download (`ingest_kaggle.py`).
2. **Transform** — Pandas cleaning & enrichment (`etl.py`, `clean_sales`).
3. **Load** — SQLite staging table, then mart aggregates (`etl.py`, SQL under `sql/`).
4. **Metadata** — Source fingerprint, pipeline runs, alerts (`meta.py` + `sql/meta_*.sql`).
5. **Monitor** — Compare fingerprints, schema checks (`monitor.py`).
6. **Analytics** — Parameterized SQL files + pandas (`analytics.py`, `RetailAnalytics`).
7. **Serve** — This dashboard calls the analytics layer and renders Plotly.
""",
        ),
    ]
    for title, md in steps:
        with st.expander(title, expanded=True):
            st.markdown(md)

    st.markdown("### Step 3 — Module & file reference")

    with st.expander("Folder: project root", expanded=False):
        st.markdown(
            """
- **`app.py`** — Dashboard: load marts, KPIs, charts, manual monitor trigger.
- **`Dockerfile`** — Python 3.12 slim image, `pip install`, `streamlit run`.
- **`.dockerignore`** — Shrinks build context.
- **`requirements.txt` / `requirements-dev.txt`** — Runtime vs dev (pytest).
- **`pyproject.toml`** — Setuptools package layout.
- **`pytest.ini`** — Test paths / `pythonpath`.
- **`README.md`** — Project documentation.
- **`.env.example`** — Environment variable template.
"""
        )

    with st.expander("Folder: `src/retail_etl/` (core package)", expanded=detail_open):
        st.markdown(
            "Business logic lives here. The UI should not embed long SQL strings—call these layers instead."
        )
        pkg_files: list[tuple[str, str, str]] = [
            ("__init__.py", "Package marker.", "Standard Python package; enables `import retail_etl`."),
            (
                "settings.py",
                "Environment + path config.",
                "`ProjectPaths`, `get_paths()`, and `Settings.load()`: data/db/reports paths, DB path, defaults, log level.",
            ),
            (
                "utils.py",
                "Shared helpers.",
                "`configure_logging`, `get_logger`, `load_sql_section` (cached bundle loader) for runtime modules.",
            ),
            (
                "db_security.py",
                "Table allowlists.",
                "`EXPECTED_RAW_COLUMNS`, `assert_read_table`, `assert_export_table` — guard dynamic SQL.",
            ),
            (
                "etl.py",
                "ETL core.",
                "`load_raw_csv`, `clean_sales`, `write_sqlite`, `run_etl`, `rebuild_marts`; SQLite transactions.",
            ),
            (
                "meta.py",
                "SQLite metadata tables.",
                "Source state, schema snapshot, pipeline runs, alerts via `meta.sql` sections.",
            ),
            (
                "ingest_kaggle.py",
                "Kaggle download.",
                "Slug validation, SHA256 fingerprint, lazy `KaggleApi` import.",
            ),
            (
                "monitor.py",
                "Change detection.",
                "Fingerprint vs stored state; schema vs expected columns; optional ETL refresh.",
            ),
            (
                "analytics.py",
                "Analytics layer.",
                "`RetailAnalytics`: KPIs, weekday revenue, invoice distribution, RFM; SQL in `analytics_*.sql`.",
            ),
            ("exporter.py", "Export marts.", "Allowlisted table reads, multiple formats."),
            ("plotting.py", "Offline charts.", "Plotly HTML/PNG to `reports/charts`."),
            ("cli.py", "CLI.", "`python -m retail_etl.cli` — ingest, monitor, watch, run-all, …"),
            (
                "presentation.py",
                "Dashboard copy.",
                "Architecture narrative and light CSS for this app (separated from UI logic).",
            ),
        ]
        for name, short, long in pkg_files:
            with st.expander(f"File: `retail_etl/{name}`", expanded=False):
                st.markdown(f"**Summary:** {short}\n\n**Detail:** {long}")

    with st.expander("Folder: `src/retail_etl/sql/`", expanded=False):
        st.markdown(
            "Five bundled `.sql` files group related statements. Sections are marked with "
            "`-- @section name` … `-- @end`; Python loads them via `utils.load_sql_section(bundle, section)`."
        )
        sql_bundles: list[tuple[str, str]] = [
            (
                "meta.sql",
                "Metadata: `init_tables`, `upsert_source_state`, `get_source_state`, alerts, `start_run` / `finish_run`, schema snapshot, `get_last_success`.",
            ),
            (
                "etl.sql",
                "Staging lifecycle: `init_staging`, `create_unique_index`, `drop_staging`, `insert_incremental`, `select_max_invoice_date`.",
            ),
            (
                "marts.sql",
                "Mart rebuild SELECTs: `monthly`, `product`, `country`, `customer` → `mart_*` tables.",
            ),
            (
                "analytics.sql",
                "Dashboard analytics: `kpis`, `weekday`, `invoice_distribution`, `rfm_max_date`, `rfm_customers`.",
            ),
            ("app.sql", "Streamlit helpers: `dataset_overview`, `staging_for_slicers`."),
        ]
        for fname, desc in sql_bundles:
            with st.expander(f"SQL bundle: `{fname}`", expanded=False):
                st.markdown(desc)
                sql_path = root / "src" / "retail_etl" / "sql" / fname
                if sql_path.is_file():
                    st.code(sql_path.read_text(encoding="utf-8"), language="sql")

    with st.expander("Folder: `tests/`", expanded=False):
        st.markdown(
            """
- **`test_clean_sales.py`** — `clean_sales` behaviour.
- **`test_sql_guard.py`** — Table allowlist rejects unknown names.
"""
        )

    with st.expander("Folders: `data/raw` & `data/db`", expanded=False):
        st.markdown(
            """
- **`data/raw/`** — Source CSV (often gitignored).
- **`data/db/retail.db`** — SQLite after ETL: staging, marts, meta.
- **`data/exports/`** — CLI export target (optional).
"""
        )
