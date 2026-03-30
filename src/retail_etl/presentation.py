"""Dashboard presentation copy and architecture walkthrough (English, LTR)."""

from __future__ import annotations

from pathlib import Path

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
</style>
"""


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
                "paths.py",
                "Canonical paths.",
                "`ProjectPaths` / `get_paths()`: `data/raw`, `data/db`, `reports`, etc.",
            ),
            (
                "settings.py",
                "Environment-driven config.",
                "`Settings.load()`: DB path, default CSV, log level, Kaggle defaults.",
            ),
            ("utils.py", "Logging helpers.", "`configure_logging`, `get_logger` for CLI and library."),
            (
                "db_security.py",
                "Table allowlists.",
                "`EXPECTED_RAW_COLUMNS`, `assert_read_table`, `assert_export_table` — guard dynamic SQL.",
            ),
            (
                "sql_loader.py",
                "Load `.sql` files.",
                "`load_sql(name)` reads from `sql/` with LRU cache.",
            ),
            (
                "etl.py",
                "ETL core.",
                "`load_raw_csv`, `clean_sales`, `write_sqlite`, `run_etl`, `rebuild_marts`; SQLite transactions.",
            ),
            (
                "meta.py",
                "SQLite metadata tables.",
                "Source state, schema snapshot, pipeline runs, alerts via `meta_*.sql`.",
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
        st.markdown("Each file is raw SQL; Python loads it at runtime via `sql_loader.load_sql`.")
        sql_index: list[tuple[str, str]] = [
            ("marts_monthly.sql", "Monthly aggregates → revenue trend."),
            ("marts_product.sql", "Revenue by product (code + description)."),
            ("marts_country.sql", "Revenue by country."),
            ("marts_customer.sql", "Revenue by customer."),
            ("analytics_kpis.sql", "Dashboard KPIs from staging."),
            ("analytics_weekday.sql", "Revenue by weekday (`strftime`)."),
            ("analytics_invoice_distribution.sql", "Revenue per invoice."),
            ("analytics_rfm_max_date.sql", "Max invoice date for recency baseline."),
            ("analytics_rfm_customers.sql", "Per-customer RFM base metrics."),
            ("etl_init_staging.sql", "Create clean staging table."),
            ("etl_create_unique_index.sql", "Unique index for incremental dedupe."),
            ("etl_drop_staging.sql", "Drop staging / index before full reload."),
            ("etl_insert_incremental.sql", "`INSERT OR IGNORE` for new rows."),
            ("etl_select_max_invoice_date.sql", "Watermark for incremental chunks."),
            ("meta_init_tables.sql", "Create all meta tables."),
            ("meta_upsert_source_state.sql", "Upsert source fingerprint."),
            ("meta_get_source_state.sql", "Read source state."),
            ("meta_add_alert.sql", "Insert alert."),
            ("meta_clear_alerts_all.sql", "Deactivate all alerts."),
            ("meta_clear_alerts_by_kind.sql", "Deactivate by kind."),
            ("meta_list_active_alerts.sql", "List active alerts."),
            ("meta_start_run.sql", "Start pipeline run row."),
            ("meta_finish_run.sql", "Finish run with status / error."),
            ("meta_upsert_schema_state.sql", "Persist column/dtype snapshot."),
            ("meta_get_last_success.sql", "Last successful run."),
        ]
        for fname, desc in sql_index:
            with st.expander(f"SQL: `{fname}`", expanded=False):
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
