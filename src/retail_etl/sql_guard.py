"""Safe SQL identifiers (allowlist only; no user-controlled table names in queries)."""

from __future__ import annotations

from .constants import ALLOWED_EXPORT_TABLES, ALLOWED_READ_TABLES


class SqlGuardError(ValueError):
    """Raised when a table name is not on the allowlist."""


def assert_allowed_table(table: str, *, allow: frozenset[str]) -> str:
    if table not in allow:
        raise SqlGuardError(f"Table not allowed: {table!r}")
    return table


def assert_read_table(table: str) -> str:
    return assert_allowed_table(table, allow=ALLOWED_READ_TABLES)


def assert_export_table(table: str) -> str:
    return assert_allowed_table(table, allow=ALLOWED_EXPORT_TABLES)
