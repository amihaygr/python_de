"""Format UTC ISO timestamps from meta tables for Israel (Asia/Jerusalem) display."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from zoneinfo import ZoneInfo

_IL = ZoneInfo("Asia/Jerusalem")


def parse_utc_iso(value: Optional[str]) -> datetime | None:
    """Parse ISO-8601 from SQLite/meta; treat naive as UTC."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def format_utc_iso_as_israel(value: Optional[str], *, pattern: str = "%d/%m/%Y %H:%M") -> str:
    """Human-readable local time in Israel (IST/IDT). Returns em dash if unparsable."""
    dt = parse_utc_iso(value)
    if dt is None:
        return "—"
    local = dt.astimezone(_IL)
    tz = local.tzname() or ""
    return f"{local.strftime(pattern)} {tz}".strip()


def localize_alert_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace created_at ISO strings with Israel-formatted strings for UI tables."""
    out: list[dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        if r.get("created_at"):
            r["created_at"] = format_utc_iso_as_israel(str(r["created_at"]))
        out.append(r)
    return out
