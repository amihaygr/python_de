from __future__ import annotations

from retail_etl.local_time import (
    format_utc_iso_as_israel,
    format_utc_iso_as_israel_compact,
    parse_utc_iso,
)


def test_parse_utc_z_suffix() -> None:
    dt = parse_utc_iso("2026-03-26T18:30:00+00:00")
    assert dt is not None
    assert dt.hour == 18


def test_format_israel_not_empty() -> None:
    s = format_utc_iso_as_israel("2026-06-15T12:00:00+00:00")
    assert s != "—"
    assert "2026" in s or "15" in s


def test_format_invalid_returns_dash() -> None:
    assert format_utc_iso_as_israel("") == "—"
    assert format_utc_iso_as_israel("not-a-date") == "—"


def test_format_compact_no_tz_suffix() -> None:
    s = format_utc_iso_as_israel_compact("2026-06-15T12:00:00+00:00")
    assert s != "—"
    assert "IST" not in s and "IDT" not in s
    assert "2026" in s
