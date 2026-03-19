"""Runtime configuration from environment variables (12-factor style)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .paths import ProjectPaths, get_paths


@dataclass(frozen=True)
class Settings:
    """Application settings loaded once per process."""

    db_path: Path
    raw_csv_default: Path
    log_level: str
    default_kaggle_dataset: str
    default_kaggle_filename: str

    @staticmethod
    def load(paths: ProjectPaths | None = None) -> Settings:
        p = paths or get_paths()
        db = os.environ.get("RETAIL_ETL_DB_PATH")
        raw = os.environ.get("RETAIL_ETL_RAW_CSV")
        return Settings(
            db_path=Path(db).expanduser() if db else (p.db_dir / "retail.db"),
            raw_csv_default=Path(raw).expanduser() if raw else (p.raw_dir / "retail_sales.csv"),
            log_level=os.environ.get("RETAIL_ETL_LOG_LEVEL", "INFO").upper(),
            default_kaggle_dataset=os.environ.get("RETAIL_KAGGLE_DATASET", "").strip(),
            default_kaggle_filename=os.environ.get("RETAIL_KAGGLE_FILENAME", "retail_sales.csv").strip()
            or "retail_sales.csv",
        )
