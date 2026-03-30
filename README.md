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
