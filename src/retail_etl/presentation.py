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

# Hebrew “what to say” blocks — shown when “Show Hebrew presenter hints” is enabled in the sidebar.
# Mirrors docs/presentation_personal_guide_he.md (keep in sync when changing demo flow).
PRESENTER_HINTS_HE: dict[str, str] = {
    "intro": """
**מטרה:** להראות שהמערכת *מנוהלת* — לא רק גרפים, אלא גם מקור, ריצות, והתראות.

**מה להצביע עליו בזה אחר זה**
1. **ארבעת המטריקות למעלה** — היקף נתונים אחרי ניקוי (שורות, לקוחות, מדינות, טווח תאריכים). אמור: “זה ה־grain שעליו כל שאר הדוחות בנויים”.
2. **שורת “Slicer result”** — מסבירה כמה שורות נשארו אחרי הסינון בצד. אמור: “כל מה שאראה מעכשיו מכבד את אותם פילטרים — בלי להריץ מחדש ETL”.
3. **Operational status** — “Last successful refresh”, מצב ריצה, מספר alerts. אמור: “גם אם הנתונים לא השתנו, בדיקה מוצלחת נרשמת — לכן התאריך מתעדכן”.
4. **טביעת אצבע / SHA** — אמור: “אפשר להוכיח בביקורת שמקור הקובץ זהה למה שטענו”.
5. **כפתור Run refresh check** — אמור: “כאן מפעילים ניטור מול Kaggle/CSV: fingerprint, סכימה, וטעינה מבוקרת”.
6. **תרשים הזרימה (Sankey) למטה** — אמור: “שלבים ממוספרים ①–⑦, רקע בהיר וטקסט כהה לקריאה מרחוק; אותו סיפור כמו במסמך ההצגה — E→T→L ופיצול ל־marts מול meta לפני הדשבורד”.
""",
    "kpis": """
**מטרה:** להראות מגמות, עונתיות, והתפלגות — *בהקשר המסונן*.

**לפי סדר המסך**
1. **Headline metrics** — Total revenue / Units / Invoices ואז ממוצעים. אמור: “אלה KPIים עסקיים ישירים מה־staging המסונן”.
2. **Monthly revenue** — קו מגמה + ממוצע נע 3 חודשים. אמור: “רואים עונתיות ושבירות אם עדכנו נתונים”.
3. **Revenue by weekday** — אמור: “מזהים ימים חזקים לקמפיינים ולוגיסטיקה”.
4. **Shopping rhythm (heatmap)** — אמור: “מפת חום weekday×שעה: במצב מוחלט — כסף; במצב אחוזים — *צורת היום* גם כשההכנסה הכוללת שונה; שעות עסקים מזמינים את הקהל לחלון הרלוונטי”.
5. **Invoice size distribution** — היסטוגרמה + box. אמור: “רואים אם יש עסקאות ‘ענק’ נדירות או עקביות — חשוב לסיכון אשראי/הנחות”.
""",
    "products": """
**מטרה:** ריכוזיות מוצרים (Pareto).

**מה להגיד**
- “בר גרף אופקי של Top N מוצרים לפי revenue”. שנה את **Top N** כדי להראות גמישות.
- “אם כמה מוצרים שולטים בהכנסות — יש תלות במלאי ובמחסן; אם התפלגות שטוחה — פורטפוליו מגוון”.
- אם מצב **Technical**: “הטבלה למטה מראה את אותם מספרים בצורה מדויקת”.
""",
    "customers": """
**מטרה:** ריכוזיות לקוחות וסיכון ריכוז.

**מה להגיד**
- “אותו סלייסר גלובלי — אז ההשוואה לטאב מוצרים עקבית”.
- “לקוחות בראש הרשימה דורשים שימור; שים לב אם יש לקוח בודד עם נתח חריג”.
- “מקשרים לטאב RFM לפעולות שיווק ממוקדות”.
""",
    "countries": """
**מטרה:** גיאוגרפיה ונתח שוק.

**מה להגיד**
- **Treemap** — “גודל המלבן ∝ הכנסה; צבע מחזק את ההבדל”.
- “בדאטה הזה UK לרוב דומיננטי — אפשר לשאול מה הפוטנציאל בחו״ל”.
- שנה **Top N** כדי להראות עומק גיאוגרפי.
""",
    "rfm": """
**מטרה:** סגמנטציה לפי Recency / Frequency / Monetary — *על הנתונים המסוננים*.

**מה להגיד**
1. **הסליידרים** — “מצמצמים לקוחות רלוונטיים: לא ישנים מדי, לא חד־פעמיים מדי, לא זניחים כספית”.
2. **גרף עמודות סגמנטים** — “איזה קודי RFM הכי מייצגים את הבייס”.
3. **בועות** — “ציר X רצות ימים מאז רכישה אחרונה, Y תדירות, גודל בועה = כסף”.
4. “הלוגיקה בקוד נשארת ב־`RetailAnalytics` / פונקציות עזר — ה־UI נשאר דק”.
""",
    "arch": """
**מטרה:** שקיפות הנדסית — מבנה תיקיות, SQL חיצוני, בדיקות.

**מה להגיד**
- “מפרידים UI מלוגיקה: `app.py` מציג, `retail_etl` מבצע”.
- “כל שאילתה דינמית עוברת allowlist; SQL בקבצים לתחזוקה”.
- פתח expander אחד כדוגמה: “כך מאשרים שינוי בקוד בלי לחפש מחרוזות בפייתון”.
""",
    "summary": """
**מטרה:** סגירה מהירה — value proposition.

**מה להגיד**
- **Executive:** “מסלול אחד מנוהל מקובץ גולמי לדוחות שאפשר להגן עליהם בפגישת הנהלה”.
- **Technical:** “Docker, pytest, מטא־דאטה, CLI — מוכן לסביבת קורס/POC ולהרחבה עתידית”.
""",
    "staging": """
**מטרה:** הוכחת grain וחוזה נתונים.

**מה להגיד**
- “כל שורה = שורת חשבונית; `line_total` הוא פיצ’ר מחושב ב־ETL”.
- “התצוגה מכבדת slicers — אותה לוגיקה כמו בגרפים”.
- “אם שואלים על ניקוי — להפנות ל־`CleanConfig` ולסינון כמויות/מחירים”.
""",
}


def render_hebrew_presenter_hint(st, tab_key: str, *, enabled: bool) -> None:
    """Expandable Hebrew script for live demos (see PRESENTER_HINTS_HE)."""
    if not enabled:
        return
    body = PRESENTER_HINTS_HE.get(tab_key)
    if not body:
        return
    with st.expander("מה להגיד עכשיו · עברית (טקסט להצגה)", expanded=False):
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
        "Flows are **even widths** (illustrative). Read the **numbered** stages left → right; staging splits into "
        "**marts** vs **meta**, then both feed the dashboard."
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
                "`configure_logging`, `get_logger`, `load_sql` (cached SQL loader) for runtime modules.",
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
        st.markdown("Each file is raw SQL; Python loads it at runtime via `utils.load_sql`.")
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
