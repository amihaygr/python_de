"""Shared helpers (logging, SQL bundle loader)."""

from __future__ import annotations

import logging
import sys
from functools import lru_cache
from pathlib import Path


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger once (repeat calls only adjust level)."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(numeric)
        return
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def _sql_dir() -> Path:
    return Path(__file__).resolve().parent / "sql"


def _parse_sql_bundle(content: str) -> dict[str, str]:
    """Split a bundled .sql file into sections marked by `-- @section name` … `-- @end`."""
    sections: dict[str, str] = {}
    current: str | None = None
    buf: list[str] = []
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- @section "):
            if current is not None:
                sections[current] = "\n".join(buf).strip()
            current = stripped[len("-- @section ") :].strip()
            buf = []
        elif stripped == "-- @end":
            if current is not None:
                sections[current] = "\n".join(buf).strip()
            current = None
            buf = []
        else:
            buf.append(line)
    if current is not None:
        sections[current] = "\n".join(buf).strip()
    return {k: v for k, v in sections.items() if v}


@lru_cache(maxsize=16)
def _bundle_sections(bundle_filename: str) -> dict[str, str]:
    path = _sql_dir() / bundle_filename
    text = path.read_text(encoding="utf-8")
    return _parse_sql_bundle(text)


def load_sql_section(bundle_filename: str, section: str) -> str:
    """Load one named section from a bundled SQL file under `retail_etl/sql/`."""
    sections = _bundle_sections(bundle_filename)
    if section not in sections:
        available = ", ".join(sorted(sections))
        raise KeyError(f"Unknown SQL section {section!r} in {bundle_filename}. Available: {available}")
    return sections[section]


def clear_sql_bundle_cache() -> None:
    """Invalidate bundle cache (e.g. after tests mutate files — rarely needed)."""
    _bundle_sections.cache_clear()
