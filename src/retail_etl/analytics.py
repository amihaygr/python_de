"""שכבת אנליטיקה עסקית מעל SQLite (שאילתות + Pandas)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import sqlite3

from .utils import get_logger, load_sql

logger = get_logger(__name__)


def weekday_hour_revenue_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """Rows: weekday (Sun..Sat), columns: hour 0–23, values: sum of line revenue."""
    if df.empty or not {"InvoiceDate", "line_total"}.issubset(df.columns):
        return pd.DataFrame()
    work = df[["InvoiceDate", "line_total"]].copy()
    work["line_total"] = pd.to_numeric(work["line_total"], errors="coerce").fillna(0.0)
    work["hour"] = work["InvoiceDate"].dt.hour
    work["weekday_short"] = work["InvoiceDate"].dt.day_name().str.slice(0, 3)
    g = work.groupby(["weekday_short", "hour"], as_index=False).agg(revenue=("line_total", "sum"))
    if g.empty:
        return pd.DataFrame()
    pivot = g.pivot(index="weekday_short", columns="hour", values="revenue").fillna(0.0)
    weekday_order = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    pivot = pivot.reindex([w for w in weekday_order if w in pivot.index])
    for h in range(24):
        if h not in pivot.columns:
            pivot[h] = 0.0
    pivot = pivot[sorted(pivot.columns)]
    return pivot


def _connect(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path, timeout=30.0)


@dataclass(frozen=True)
class RfmRow:
    customer_id: int
    recency_days: float
    frequency: int
    monetary: float
    r_score: int
    f_score: int
    m_score: int
    rfm_segment: str


class RetailAnalytics:
    """אנליטיקה מרוכזת: KPI, הכנסות לפי יום, התפלגות חשבוניות, RFM."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def _read_sql(self, sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
        with _connect(self.db_path) as conn:
            return pd.read_sql_query(sql, conn, params=params)

    def get_kpis(self) -> dict[str, float | str]:
        """מחזיר מילון KPI מהטבלה הנקייה."""
        sql = load_sql("analytics_kpis.sql")
        row = self._read_sql(sql).iloc[0].to_dict()
        return {
            "revenue": float(row["revenue"] or 0),
            "units": float(row["units"] or 0),
            "invoices": float(row["invoices"] or 0),
            "customers": float(row["customers"] or 0),
            "products": float(row["products"] or 0),
            "countries": float(row["countries"] or 0),
            "line_items": float(row["line_items"] or 0),
            "min_date": str(row["min_date"]),
            "max_date": str(row["max_date"]),
            "avg_invoice_value": float(row["avg_invoice_value"] or 0),
            "avg_lines_per_invoice": float(row["avg_lines_per_invoice"] or 0),
            "avg_spend_per_customer": float(row["avg_spend_per_customer"] or 0),
            "uk_revenue_share": float(row["uk_revenue_share"] or 0),
        }

    def get_revenue_by_weekday(self) -> pd.DataFrame:
        """הכנסה מצטברת לפי יום בשבוע (מספר 0=ראשון … 6=שבת)."""
        sql = load_sql("analytics_weekday.sql")
        df = self._read_sql(sql)
        # תוויות באנגלית קצרות — הממשק יכול למפות לעברית לתצוגה
        names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        df["weekday"] = df["weekday_num"].apply(lambda x: names[int(x)] if pd.notna(x) else None)
        return df

    def get_invoice_revenue_distribution(self) -> pd.DataFrame:
        """סכום הכנסה לכל חשבונית (שורה לכל InvoiceNo)."""
        sql = load_sql("analytics_invoice_distribution.sql")
        df = self._read_sql(sql)
        df["invoice_revenue"] = pd.to_numeric(df["invoice_revenue"], errors="coerce").fillna(0.0)
        return df

    def get_rfm(self, *, q: int = 5) -> pd.DataFrame:
        """RFM לכל לקוח: עדכנות (ימים), תדירות (חשבוניות), כסף; ציונים וסגמנט."""
        sql_max = load_sql("analytics_rfm_max_date.sql")
        max_date_str = self._read_sql(sql_max).iloc[0]["max_date"]
        if pd.isna(max_date_str):
            return pd.DataFrame()

        max_dt = pd.to_datetime(max_date_str)

        sql = load_sql("analytics_rfm_customers.sql")
        df = self._read_sql(sql)
        df["last_invoice"] = pd.to_datetime(df["last_invoice"], errors="coerce")
        df["recency_days"] = (max_dt - df["last_invoice"]).dt.total_seconds() / 86400.0
        df["frequency"] = pd.to_numeric(df["frequency"], errors="coerce").fillna(0).astype(int)
        df["monetary"] = pd.to_numeric(df["monetary"], errors="coerce").fillna(0.0)

        def _qcut_codes(values: pd.Series) -> tuple[int, pd.Series]:
            """מחזיר (מספר_תאים, קודים) אחרי qcut; מתמודד עם כפילויות בערכים."""
            try:
                codes = pd.qcut(values, q=q, labels=False, duplicates="drop")
            except ValueError:
                # כל הערכים זהים או מקרה קצה — תא יחיד
                codes = pd.Series([0] * len(values), index=values.index, dtype="int64")
            codes = codes.fillna(0).astype(int)
            n_bins = int(codes.max()) + 1 if len(codes) else 1
            n_bins = max(n_bins, 1)
            return n_bins, codes

        # עדכנות: פחות ימים = טוב יותר → הופכים את קוד התאים
        r_bins, r_codes = _qcut_codes(df["recency_days"].fillna(0))
        f_bins, f_codes = _qcut_codes(df["frequency"].fillna(0))
        m_bins, m_codes = _qcut_codes(df["monetary"].fillna(0.0))

        df["r_score"] = (r_bins - r_codes).clip(lower=1).astype(int)
        df["f_score"] = (f_codes + 1).astype(int)
        df["m_score"] = (m_codes + 1).astype(int)
        df["rfm_segment"] = df["r_score"].astype(str) + df["f_score"].astype(str) + df["m_score"].astype(str)

        return df

