from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path

from kaggle.api.kaggle_api_extended import KaggleApi

from .logging_config import get_logger
from .paths import get_paths

logger = get_logger(__name__)

_KAGGLE_SLUG_RE = re.compile(r"^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$")


@dataclass(frozen=True)
class FileFingerprint:
    path: Path
    size_bytes: int
    sha256: str


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _ensure_kaggle_auth_hint() -> None:
    # KaggleApi reads credentials from:
    # - environment variables: KAGGLE_USERNAME / KAGGLE_KEY
    # - or: ~/.kaggle/kaggle.json
    if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
        return
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    if kaggle_json.exists():
        return
    raise RuntimeError(
        "Kaggle credentials not found. Set KAGGLE_USERNAME/KAGGLE_KEY or create ~/.kaggle/kaggle.json."
    )


def _validate_dataset_slug(dataset: str) -> None:
    if not _KAGGLE_SLUG_RE.match(dataset.strip()):
        raise ValueError(
            f"Invalid Kaggle dataset slug {dataset!r}. Expected format: owner/dataset-name"
        )


def download_dataset_file(
    dataset: str,
    filename: str,
    dest_path: Path | None = None,
    force: bool = False,
) -> FileFingerprint:
    """
    Download a specific file from a Kaggle dataset.

    Args:
        dataset: Kaggle dataset identifier, e.g. 'carrie1/ecommerce-data'
        filename: File name inside the dataset, e.g. 'data.csv'
        dest_path: Destination file path. Defaults to data/raw/<filename>
        force: Re-download even if file exists.
    """
    _validate_dataset_slug(dataset)
    if not filename or filename.strip() != filename or ".." in filename or "/" in filename or "\\" in filename:
        raise ValueError(f"Unsafe or invalid filename: {filename!r}")

    _ensure_kaggle_auth_hint()
    paths = get_paths()
    if dest_path is None:
        dest_path = paths.raw_dir / filename
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    api = KaggleApi()
    api.authenticate()
    logger.info("Downloading Kaggle file %s from dataset %s", filename, dataset)

    # Kaggle downloads into a directory, keeping the same filename.
    tmp_dir = dest_path.parent
    if force and dest_path.exists():
        dest_path.unlink()

    api.dataset_download_file(dataset, file_name=filename, path=str(tmp_dir), force=force, quiet=False)

    # KaggleApi may create a .zip for some datasets; if so, this will not exist.
    if not dest_path.exists():
        # Try common zip output name: <filename>.zip
        zip_path = dest_path.with_suffix(dest_path.suffix + ".zip")
        if zip_path.exists():
            import zipfile

            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(tmp_dir)
            zip_path.unlink(missing_ok=True)

    if not dest_path.exists():
        raise FileNotFoundError(f"Download succeeded but {dest_path} was not created. Check dataset files.")

    fp = FileFingerprint(
        path=dest_path,
        size_bytes=dest_path.stat().st_size,
        sha256=_sha256_file(dest_path),
    )
    return fp

