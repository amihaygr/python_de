from __future__ import annotations

import pandas as pd

from retail_etl.etl import CleanConfig, clean_sales


def test_clean_sales_basic() -> None:
    df = pd.DataFrame(
        {
            "InvoiceNo": ["A1", "A2"],
            "StockCode": ["S1", "S2"],
            "Description": ["x", None],
            "Quantity": [2, 1],
            "InvoiceDate": ["01/12/2010 08:26", "02/12/2010 09:00"],
            "UnitPrice": [2.5, 3.0],
            "CustomerID": [1.0, 2.0],
            "Country": ["UK", "France"],
        }
    )
    out = clean_sales(df, cfg=CleanConfig())
    assert len(out) == 2
    assert "line_total" in out.columns
    assert out["line_total"].iloc[0] == 5.0


def test_clean_sales_drops_bad_rows() -> None:
    df = pd.DataFrame(
        {
            "InvoiceNo": ["A1", "A2", "A3"],
            "StockCode": ["S1", "S2", "S3"],
            "Description": ["a", "b", "c"],
            "Quantity": [1, 0, 1],
            "InvoiceDate": ["01/12/2010 08:26", "01/12/2010 08:26", "bad"],
            "UnitPrice": [1.0, 1.0, 1.0],
            "CustomerID": [1.0, 1.0, 2.0],
            "Country": ["UK", "UK", "UK"],
        }
    )
    out = clean_sales(df, cfg=CleanConfig())
    assert len(out) == 1
