"""רשימות מותרות לשמות טבלאות ועמודות CSV צפויות.

מונע שימוש בשם טבלה שאינו ברשימה כשבונים שאילתות דינמיות.
"""

from __future__ import annotations


class SqlGuardError(ValueError):
    """שם טבלה שאינו ברשימת המותרים."""


# עמודות צפויות בקובץ CSV בסגנון Online Retail
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


# טבלאות שהדשבורד רשאי לקרוא
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

# טבלאות מותרות לייצוא (קובץ/דוח)
ALLOWED_EXPORT_TABLES: frozenset[str] = frozenset(
    {
        "mart_sales_monthly",
        "mart_product_summary",
        "mart_country_summary",
        "mart_customer_summary",
    }
)


def _assert_allowed_table(table: str, *, allow: frozenset[str]) -> str:
    if table not in allow:
        raise SqlGuardError(f"טבלה לא מותרת: {table!r}")
    return table


def assert_read_table(table: str) -> str:
    return _assert_allowed_table(table, allow=ALLOWED_READ_TABLES)


def assert_export_table(table: str) -> str:
    return _assert_allowed_table(table, allow=ALLOWED_EXPORT_TABLES)

