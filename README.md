## Retail Sales ETL Project

Python ETL project for the Naya College Data Engineering course, based on the Online Retail
Online Retail–style CSV (default: `online_retail.csv` from `dp1224/online-retail-csv` on Kaggle).

### Goals (from the presentation)

- **Design and implement an ETL end-to-end** using Pandas and SQLite.
- **Practice Python project development** with a clean `src/` layout and CLI.
- **Practice Pandas capabilities** for cleaning, joining, grouping, and aggregation.
- **Experiment with real-life data** from retail transactions.

### Project structure

- `data/raw/retail_sales.csv` – original Kaggle CSV (not tracked in git).
- `data/db/retail.db` – SQLite database with staging and mart tables.
- `data/exports/` – CSV/JSON/Parquet/XLSX exports of mart tables.
- `reports/charts/` – Plotly charts (HTML + PNG).
- `src/retail_etl/` – ETL, exports, plotting, and CLI.

### Installation

```bash
pip install -r requirements.txt
```

### Kaggle API setup (optional)

If you want the project to **download data automatically** from Kaggle, configure credentials in one of these ways:

- **Environment variables**:
  - `KAGGLE_USERNAME`
  - `KAGGLE_KEY` (create at [Kaggle → Settings → API](https://www.kaggle.com/settings))
- **Kaggle JSON file**:
  - `C:\\Users\\<you>\\.kaggle\\kaggle.json` (Windows)
  - `~/.kaggle/kaggle.json` (macOS/Linux)
- **Project `.env`**: copy `.env.example` to `.env`, set `KAGGLE_USERNAME` / `KAGGLE_KEY` there. The app and CLI load `.env` on startup via `python-dotenv` (see `retail_etl.settings`).
- **Docker**: the container does not include your home directory. Either pass `-e KAGGLE_USERNAME=... -e KAGGLE_KEY=...`, or mount your token folder read-only, for example  
  `-v %USERPROFILE%\.kaggle:/root/.kaggle:ro` (Windows host) or `-v ~/.kaggle:/root/.kaggle:ro` (Linux/macOS).

### CLI usage

- **Full pipeline** (Extract → Transform → Load → Export → Plot):

```bash
python -m retail_etl.cli run-all
```

This command:

- Loads `data/raw/online_retail.csv` (or `RETAIL_ETL_RAW_CSV`).
- Cleans and enriches the data with Pandas.
- Writes staging and mart tables into `data/db/retail.db`.
- Exports marts to `data/exports/` as CSV/JSON/Parquet/XLSX.
- Generates Plotly charts into `reports/charts/` as HTML + PNG.

- **Only exports (after ETL already ran)**:

```bash
python -m retail_etl.cli export
```

- **Only charts (after ETL already ran)**:

```bash
python -m retail_etl.cli plot
```

### API ingestion + monitoring

- **Download a specific dataset file from Kaggle**:

```bash
python -m retail_etl.cli ingest --dataset <owner/dataset> --filename <file.csv>
```

- **Check if the raw file changed and refresh ETL automatically**:

```bash
python -m retail_etl.cli monitor --dataset <owner/dataset> --filename <file.csv> --allow-incremental
```

- **Watch mode (poll every 5 minutes)**:

```bash
python -m retail_etl.cli watch --dataset <owner/dataset> --filename <file.csv> --interval-seconds 300
```

Monitoring writes status and alerts into SQLite meta tables:

- `meta_source_state` (fingerprint of raw file)
- `meta_schema_state` (observed CSV columns / dtypes)
- `meta_pipeline_runs` (history of ETL runs)
- `meta_alerts` (active alerts, e.g. schema change)

Optional: refresh from Kaggle before comparing fingerprints:

```bash
python -m retail_etl.cli monitor --dataset <owner/dataset> --filename <file.csv> --download --allow-incremental
```

Watch with periodic pull from Kaggle:

```bash
python -m retail_etl.cli watch --dataset <owner/dataset> --filename <file.csv> --interval-seconds 300 --pull
```

### Production-style features

- **Structured logging** to stderr (`RETAIL_ETL_LOG_LEVEL`, or `python -m retail_etl.cli --log-level DEBUG run-all` — global flags must come **before** the subcommand).
- **Configuration via env** (see `.env.example`): `RETAIL_ETL_DB_PATH`, `RETAIL_ETL_RAW_CSV`, `RETAIL_KAGGLE_*`.
- **SQL allowlists** for dashboard/exporter/plotting (no dynamic table names from untrusted input).
- **SQLite transactions** with `BEGIN IMMEDIATE` on full and incremental loads; rollback on failure.
- **Streamlit**: Plotly charts, optional `secrets.toml` defaults (copy from `.streamlit/secrets.toml.example`), Kaggle pull toggle on “Check now”.
- **Docker**: `docker build -t retail-etl .` then run with a volume for `data/`.
- **Tests**: `pip install -r requirements-dev.txt` then `pytest`.

### Streamlit dashboard

Run the dashboard:

```bash
streamlit run app.py
```

In the **Intro** tab, you can see monitoring status and trigger a manual **Check now**.

```bash
docker build -t retail-etl .
docker run --rm -p 8501:8501 -v retail_data:/app/data retail-etl
```

### Analysis highlights

The marts and charts cover the required analysis steps:

- **Observation & verification** – schema, date parsing, value ranges.
- **Cleaning** – remove invalid dates, zero/negative quantities, and rows without customer IDs.
- **Manipulation** – compute line totals and aggregate:
  - Monthly revenue and units.
  - Top products by revenue.
  - Top customers by revenue.
  - Top countries by revenue.
- **Storage** – SQLite marts and multi-format exports.
- **Presentation** – Plotly charts and textual summaries printed by the CLI.
