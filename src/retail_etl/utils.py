"""עזרי לוג לשימוש חוזר."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """מגדיר את ה-root logger פעם אחת (קריאות חוזרות רק מעדכנות רמה)."""
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

