"""Project-wide constants (schemas, allowlists)."""

from __future__ import annotations

# Expected raw CSV columns for Online Retail–style datasets.
EXPECTED_RAW_COLUMNS: tuple[str, ...] = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)

# Tables the dashboard and exporters may read (defense-in-depth; never interpolate user input).
ALLOWED_READ_TABLES: frozenset[str] = frozenset(
    {
        "stg_sales_clean",
        "mart_sales_monthly",
        "mart_product_summary",
        "mart_country_summary",
        "mart_customer_summary",
        "meta_source_state",
        "meta_pipeline_runs",
        "meta_alerts",
        "meta_schema_state",
    }
)

ALLOWED_EXPORT_TABLES: frozenset[str] = frozenset(
    {
        "mart_sales_monthly",
        "mart_product_summary",
        "mart_country_summary",
        "mart_customer_summary",
    }
)
