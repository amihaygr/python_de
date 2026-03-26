"""הגדרות זמן ריצה ממשתני סביבה."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .paths import ProjectPaths, get_paths

# Public Online Retail CSV on Kaggle (UCI-style columns). File must match dataset contents.
# Note: bekkarmerwan/... only ships an .xlsx, so API download of "retail_sales.csv" returns 404.
DEFAULT_RETAIL_KAGGLE_DATASET = "dp1224/online-retail-csv"
DEFAULT_RETAIL_KAGGLE_FILENAME = "online_retail.csv"


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
            raw_csv_default=Path(raw).expanduser() if raw else (p.raw_dir / DEFAULT_RETAIL_KAGGLE_FILENAME),
            log_level=os.environ.get("RETAIL_ETL_LOG_LEVEL", "INFO").upper(),
            default_kaggle_dataset=ds or DEFAULT_RETAIL_KAGGLE_DATASET,
            default_kaggle_filename=os.environ.get("RETAIL_KAGGLE_FILENAME", DEFAULT_RETAIL_KAGGLE_FILENAME).strip()
            or DEFAULT_RETAIL_KAGGLE_FILENAME,
        )
