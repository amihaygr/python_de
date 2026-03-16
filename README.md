## Retail Sales ETL Project

Python ETL project for the Naya College Data Engineering course, based on the Online Retail
`retail_sales.csv` dataset from Kaggle.

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

### CLI usage

- **Full pipeline** (Extract → Transform → Load → Export → Plot):

```bash
python -m retail_etl.cli run-all
```

This command:

- Loads `data/raw/retail_sales.csv`.
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
