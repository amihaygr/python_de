## Retail Sales ETL Project

Production-style Data Engineering project for the Naya College course, built around an Online Retail dataset from Kaggle.

Default Kaggle source:
- Dataset slug: `dp1224/online-retail-csv`
- File: `online_retail.csv`

---

## 1) Project objectives

- Build and operate an end-to-end ETL flow.
- Use Pandas for real-world cleaning, manipulation, and aggregation.
- Persist curated data in SQLite and serve analytics-ready marts.
- Deliver business-facing insights via Streamlit + Plotly.
- Maintain a structure that is easy to test, debug, and extend.

---

## 2) Architecture (maintainable by design)

Core principle: **separation of concerns**.

- `src/retail_etl/ingest_kaggle.py`  
  Kaggle auth, slug validation, safe download, file fingerprinting.
- `src/retail_etl/monitor.py`  
  Change detection, schema checks, ETL trigger strategy, alert lifecycle.
- `src/retail_etl/etl.py`  
  Raw read, cleaning rules, full/incremental load logic, mart rebuild.
- `src/retail_etl/analytics.py`  
  KPI/RFM business computations used by the dashboard.
- `src/retail_etl/meta.py`  
  Pipeline metadata tables (`meta_*`), runs, and alerts.
- `src/retail_etl/sql/*.sql`  
  SQL lives in SQL files (not hard-coded in Python).
- `src/retail_etl/cli.py`  
  Operational interface (`argparse`) for pipeline commands.
- `app.py`  
  Streamlit UI only: orchestration + presentation + slicers.

This keeps ETL rules, SQL, monitoring, and UI independent and maintainable.

---

## 3) Data flow

1. Extract from Kaggle or local CSV.
2. Validate and fingerprint source file.
3. Clean and normalize rows in Pandas.
4. Load to `stg_sales_clean` in SQLite (transactional).
5. Rebuild marts (`mart_*`) for analytical queries.
6. Record metadata, runs, and alerts in `meta_*`.
7. Serve dashboards and exports.

---

## 4) Dynamic analysis (advanced slicers)

The dashboard supports high-quality slicing directly in UI:

- Date range
- Country multi-select
- Minimum `line_total`
- Product text search
- RFM-specific slicers:
  - max recency
  - min frequency
  - min monetary
  - segment selection

All primary KPI/trend/top-N visuals react to the selected filters.

---

## 5) Installation

```bash
pip install -r requirements.txt
```

Optional dev tools:

```bash
pip install -r requirements-dev.txt
```

---

## 6) Kaggle credentials

Configure one of the following:

- Environment variables:
  - `KAGGLE_USERNAME`
  - `KAGGLE_KEY`
- Kaggle token file:
  - Windows: `C:\Users\<you>\.kaggle\kaggle.json`
  - macOS/Linux: `~/.kaggle/kaggle.json`
- `.env` file (copy from `.env.example`)

If running in Docker, pass env vars or mount the token folder:

`-v %USERPROFILE%\.kaggle:/root/.kaggle:ro`

---

## 7) CLI usage

Full pipeline:

```bash
python -m retail_etl.cli run-all
```

Ingestion:

```bash
python -m retail_etl.cli ingest --dataset <owner/dataset> --filename <file.csv>
```

Monitor single check:

```bash
python -m retail_etl.cli monitor --dataset <owner/dataset> --filename <file.csv> --allow-incremental
```

Monitor loop:

```bash
python -m retail_etl.cli watch --dataset <owner/dataset> --filename <file.csv> --interval-seconds 300 --pull
```

Exports and charts:

```bash
python -m retail_etl.cli export
python -m retail_etl.cli plot
```

---

## 8) Streamlit app

Local:

```bash
streamlit run app.py
```

Docker:

```bash
docker build -t retail-etl:latest .
docker run --rm -p 8501:8501 -v retail_data:/app/data retail-etl:latest
```

Primary tabs:
- Overview
- KPIs & trends
- Products
- Customers
- Countries
- RFM & analytics class
- Architecture
- Project summary
- Staging table

---

## 9) Storage and outputs

- Raw source: `data/raw/online_retail.csv`
- DB: `data/db/retail.db`
- Exports: `data/exports/` (CSV/JSON/Parquet/XLSX)
- Charts: `reports/charts/` (HTML/PNG)

---

## 10) Requirement coverage (course)

- [x] End-to-end ETL implementation
- [x] Pandas cleaning and manipulation
- [x] Storage with SQL database
- [x] Presentation with Plotly/Streamlit
- [x] CLI with `argparse`
- [x] Dynamic user-driven analysis
- [x] Real dataset ingestion (Kaggle)
- [ ] Mentor discussion / live presentation step (outside repo scope)

---

## 11) Presentation assets

- Personal Hebrew presentation guide: `docs/presentation_personal_guide_he.md`

Recommended flow in demo:
1. Overview
2. KPIs & trends
3. Products / Customers / Countries
4. RFM
5. Architecture
6. Project summary

---

## 12) Executive vs Technical mode (important)

The dashboard supports two presentation modes from the left sidebar (`Presentation mode`):

### Executive mode

Use this mode when speaking to business stakeholders, managers, or mentors who want impact first.

What it emphasizes:
- Clear business outcomes and KPI interpretation.
- Fewer implementation details.
- Concise "what it means" notes near visuals.

When to use:
- Demo opening.
- Final summary.
- Non-technical Q&A.

### Technical mode

Use this mode when speaking to engineers or when asked "how it works".

What it emphasizes:
- Architecture details and implementation notes.
- Code-level explanations (including `RetailAnalytics.get_rfm` source in-app).
- Operational behavior (monitoring, schema checks, metadata flow).

When to use:
- Architecture walkthrough.
- ETL and SQL design discussion.
- Reliability / maintainability Q&A.

### Quick recommendation

For a graded demo:
1. Start in **Executive** to frame value quickly.
2. Switch to **Technical** for architecture + engineering depth.
3. Return to **Executive** for closing summary.

---

## 13) Full presentation runbook (tab-by-tab)

Use this exact sequence to keep the story coherent and aligned with the app tabs.

### Tab 1 — `Overview` (1.5-2.5 min)

Show:
- Operational status block (last successful refresh, mode, alerts).
- Source fingerprint and last check time.
- "Run refresh check" behavior.

Say:
- "This is a governed pipeline, not just a one-time notebook."
- "We track runs, schema drift, and alerts in metadata tables."
- "A successful check updates operational state even when no data changed (`noop`)."

### Tab 2 — `KPIs & trends` (2 min)

Show:
- Headline metrics.
- Monthly trend.
- Weekday revenue.
- Invoice distribution.

Say:
- "All visuals are driven by the same curated staging grain."
- "KPIs and distributions react to slicers, so analysis is interactive and reproducible."

### Tabs 3-5 — `Products`, `Customers`, `Countries` (2 min total)

Show:
- Top-N controls.
- Revenue concentration by entity.

Say:
- "These views expose concentration risk and growth opportunities."
- "The same filter context applies, so comparisons are consistent."

### Tab 6 — `RFM & analytics class` (2 min)

Show:
- RFM segment chart.
- RFM slicers (recency/frequency/monetary/segments).

Say:
- "RFM is computed on filtered data, enabling targeted customer strategy."
- "Business logic is isolated in `RetailAnalytics` for maintainability."

### Tab 7 — `Architecture` (1-1.5 min)

Show:
- Layer separation and SQL-first structure.

Say:
- "SQL resides in `src/retail_etl/sql/*.sql`; Python orchestrates."
- "This reduces hidden logic and improves reviewability."

### Tab 8 — `Project summary` (1 min)

Show:
- Executive summary bullets.

Say:
- "The project delivers both business value and engineering discipline."

### Tab 9 — `Staging table` (optional, 30-60 sec)

Show:
- `stg_sales_clean` preview and columns.

Say:
- "This is the single source of truth for marts and dashboard outputs."

---

## 14) Why this architecture is maintainable

- SQL is versioned as SQL files, not embedded query strings.
- ETL, monitoring, analytics, and UI are separated by responsibility.
- Metadata tables provide observability (`meta_pipeline_runs`, `meta_source_state`, `meta_alerts`).
- CLI enables operational repeatability outside the UI.
- Docker provides consistent runtime and quick redeploy.
- Tests (`pytest`) protect key cleaning/security behavior.
