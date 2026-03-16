from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


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
    # This file is src/retail_etl/paths.py → project root is 3 parents up.
    return Path(__file__).resolve().parents[2]


def get_paths() -> ProjectPaths:
    return ProjectPaths(root=get_project_root())
