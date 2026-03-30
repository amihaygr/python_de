"""Tests for weekday × hour revenue pivot (shopping rhythm heatmap input)."""

from __future__ import annotations

import pandas as pd

from retail_etl.analytics import weekday_hour_revenue_pivot


def test_weekday_hour_revenue_pivot_empty() -> None:
    assert weekday_hour_revenue_pivot(pd.DataFrame()).empty


def test_weekday_hour_revenue_pivot_orders_weekdays_and_hours() -> None:
    # Monday 2024-01-01 10:00 and 11:00 UTC-style parse
    df = pd.DataFrame(
        {
            "InvoiceDate": pd.to_datetime(
                ["2024-01-01 10:00:00", "2024-01-01 11:00:00", "2024-01-02 10:00:00"]
            ),
            "line_total": [100.0, 50.0, 25.0],
        }
    )
    pivot = weekday_hour_revenue_pivot(df)
    assert not pivot.empty
    assert list(pivot.index) == ["Mon", "Tue"]
    assert pivot.loc["Mon", 10] == 100.0
    assert pivot.loc["Mon", 11] == 50.0
    assert pivot.loc["Tue", 10] == 25.0
    assert 0 in pivot.columns and 23 in pivot.columns
    assert pivot.shape[1] == 24
