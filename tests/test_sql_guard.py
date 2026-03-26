from __future__ import annotations

import pytest

from retail_etl.db_security import SqlGuardError, assert_export_table, assert_read_table


def test_allowed_read_table() -> None:
    assert assert_read_table("mart_sales_monthly") == "mart_sales_monthly"


def test_disallowed_read_table() -> None:
    with pytest.raises(SqlGuardError):
        assert_read_table("users; DROP TABLE mart_sales_monthly;--")


def test_allowed_export_table() -> None:
    assert assert_export_table("mart_product_summary") == "mart_product_summary"
