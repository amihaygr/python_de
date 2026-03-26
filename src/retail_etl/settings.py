"""הגדרות זמן ריצה ממשתני סביבה."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .paths import ProjectPaths, get_paths

# Course project default: same retail CSV as documented in README / .env.example.
DEFAULT_RETAIL_KAGGLE_DATASET = "bekkarmerwan/retail-sales-dataset-sample-transactions"
DEFAULT_RETAIL_KAGGLE_FILENAME = "retail_sales.csv"


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv()


_load_dotenv_if_available()


@dataclass(frozen=True)
class Settings:
    """ערכי קונפיגורציה לתהליך."""

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
        ds = os.environ.get("RETAIL_KAGGLE_DATASET", "").strip()
        return Settings(
            db_path=Path(db).expanduser() if db else (p.db_dir / "retail.db"),
            raw_csv_default=Path(raw).expanduser() if raw else (p.raw_dir / "retail_sales.csv"),
            log_level=os.environ.get("RETAIL_ETL_LOG_LEVEL", "INFO").upper(),
            default_kaggle_dataset=ds or DEFAULT_RETAIL_KAGGLE_DATASET,
            default_kaggle_filename=os.environ.get("RETAIL_KAGGLE_FILENAME", DEFAULT_RETAIL_KAGGLE_FILENAME).strip()
            or DEFAULT_RETAIL_KAGGLE_FILENAME,
        )
