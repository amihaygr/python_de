"""הגדרות זמן ריצה ממשתני סביבה."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Public Online Retail CSV on Kaggle (UCI-style columns). File must match dataset contents.
# Note: bekkarmerwan/... only ships an .xlsx, so API download of "retail_sales.csv" returns 404.
DEFAULT_RETAIL_KAGGLE_DATASET = "dp1224/online-retail-csv"
DEFAULT_RETAIL_KAGGLE_FILENAME = "online_retail.csv"


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def db_dir(self) -> Path:
        return self.data_dir / "db"

    @property
    def exports_dir(self) -> Path:
        return self.data_dir / "exports"

    @property
    def reports_dir(self) -> Path:
        return self.root / "reports"

    @property
    def charts_dir(self) -> Path:
        return self.reports_dir / "charts"


def get_project_root() -> Path:
    # this file is in src/retail_etl/settings.py -> project root is parents[2]
    return Path(__file__).resolve().parents[2]


def get_paths() -> ProjectPaths:
    return ProjectPaths(root=get_project_root())


def _load_dotenv_if_available() -> None:
    """Load `.env` from project root so Kaggle/DB vars work even when cwd is not the repo."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_path = get_project_root() / ".env"
    load_dotenv(env_path)
    load_dotenv()  # optional: cwd `.env` if present (does not unset vars already set)


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
