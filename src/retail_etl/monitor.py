"""זיהוי שינוי בקובץ גולמי, השוואת סכימה והפעלת ETL לפי הצורך."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from .db_security import EXPECTED_RAW_COLUMNS
from .etl import CleanConfig, run_etl
from .ingest_kaggle import FileFingerprint, _sha256_file
from .utils import get_logger
from .meta import (
    add_alert,
    clear_alerts,
    connect,
    finish_run,
    get_last_success,
    get_source_state,
    init_meta_tables,
    start_run,
    upsert_schema_state,
    upsert_source_state,
)
from .paths import get_paths

logger = get_logger(__name__)


@dataclass(frozen=True)
class Profile:
    """תמונת מצב של קובץ CSV (עמודות, טיפוסים לדוגמה, מספר שורות)."""
    columns: list[str]
    dtypes: dict[str, str]
    row_count: int
    max_invoice_date: Optional[str]


def fingerprint_file(path: Path) -> FileFingerprint:
    return FileFingerprint(path=path, size_bytes=path.stat().st_size, sha256=_sha256_file(path))


def profile_csv(csv_path: Path, chunksize: int = 200_000) -> Profile:
    # כותרות בלבד
    header = pd.read_csv(csv_path, nrows=0)
    columns = [c for c in header.columns if c != "index"]

    # טיפוסים משוערים מדגימה קטנה
    sample = pd.read_csv(csv_path, nrows=10_000)
    if "index" in sample.columns:
        sample = sample.drop(columns=["index"])
    dtypes = {c: str(sample[c].dtype) for c in sample.columns}

    # ספירת שורות ותאריך מקסימלי בצ'אנקים
    row_count = 0
    max_dt = None
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        if "index" in chunk.columns:
            chunk = chunk.drop(columns=["index"])
        row_count += len(chunk)
        if "InvoiceDate" in chunk.columns:
            dt = pd.to_datetime(chunk["InvoiceDate"], dayfirst=True, errors="coerce")
            cur_max = dt.max()
            if pd.notna(cur_max):
                max_dt = cur_max if max_dt is None else max(max_dt, cur_max)

    max_invoice_date = None if max_dt is None else max_dt.isoformat()
    return Profile(columns=columns, dtypes=dtypes, row_count=row_count, max_invoice_date=max_invoice_date)


def check_for_update(
    *,
    dataset: str,
    filename: str,
    csv_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    allow_incremental: bool = True,
    download_first: bool = False,
) -> dict:
    """משווה טביעת אצבע לקובץ הנוכחי מול המטא; מחזיר מילון סטטוס לדשבורד."""
    paths = get_paths()
    if csv_path is None:
        csv_path = paths.raw_dir / filename
    if db_path is None:
        db_path = paths.db_dir / "retail.db"

    missing = not csv_path.exists()
    # First run: no local raw file yet — pull from Kaggle if credentials + slug are configured.
    need_download = download_first or missing
    if need_download:
        from .ingest_kaggle import download_dataset_file

        logger.info(
            "Downloading from Kaggle: %s / %s (missing=%s, user_requested=%s)",
            dataset,
            filename,
            missing,
            download_first,
        )
        download_dataset_file(
            dataset=dataset,
            filename=filename,
            dest_path=csv_path,
            force=download_first,
        )

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found at {csv_path}. "
            "Configure Kaggle credentials, set dataset slug + filename in the sidebar, "
            "or run: python -m retail_etl.cli ingest --dataset <owner/dataset> --filename <file.csv>"
        )

    fp = fingerprint_file(csv_path)

    with connect(db_path) as conn:
        init_meta_tables(conn)
        prev = get_source_state(conn)

        # No previous state: run full ETL and record.
        if prev is None or prev.get("sha256") is None:
            clear_alerts(conn, kind="schema_change")
            run = start_run(conn, mode="full")
            try:
                run_etl(csv_path=csv_path, db_path=db_path, mode="full")
                upsert_source_state(conn, dataset, filename, fp.size_bytes, fp.sha256)
                prof0 = profile_csv(csv_path)
                upsert_schema_state(
                    conn,
                    columns_json=json.dumps(prof0.columns),
                    dtypes_json=json.dumps(prof0.dtypes),
                )
                finish_run(conn, run.run_id, status="success", rows_written=0)
            except Exception as e:  # noqa: BLE001
                add_alert(conn, "etl_failure", str(e))
                finish_run(conn, run.run_id, status="failed", error=str(e))
                raise
            logger.info("Initial full ETL completed; schema fingerprint stored.")
            return {"action": "full_refresh", "changed": True}

        changed = fp.sha256 != prev["sha256"] or fp.size_bytes != prev["size_bytes"]
        if not changed:
            logger.debug("No change detected for %s (sha256 unchanged).", csv_path)
            return {"action": "noop", "changed": False}

        # Profile to detect schema changes and row append.
        prof = profile_csv(csv_path)
        expected_columns = list(EXPECTED_RAW_COLUMNS)
        schema_ok = prof.columns == expected_columns
        upsert_schema_state(
            conn,
            columns_json=json.dumps(prof.columns),
            dtypes_json=json.dumps(prof.dtypes),
        )
        if not schema_ok:
            add_alert(
                conn,
                "schema_change",
                f"Schema changed. Expected {expected_columns} but got {prof.columns}",
            )
            return {"action": "schema_alert", "changed": True, "profile": json.dumps(prof.__dict__)}

        clear_alerts(conn, kind="schema_change")

        # Decide incremental vs full (incremental only if last run exists and table exists)
        last_success = get_last_success(conn)
        mode = "full"
        if allow_incremental and last_success is not None:
            mode = "incremental"

        run = start_run(conn, mode=mode)
        try:
            run_etl(csv_path=csv_path, db_path=db_path, mode=mode, cfg=CleanConfig())
            upsert_source_state(conn, dataset, filename, fp.size_bytes, fp.sha256)
            finish_run(conn, run.run_id, status="success", rows_written=0)
        except Exception as e:  # noqa: BLE001
            add_alert(conn, "etl_failure", str(e))
            finish_run(conn, run.run_id, status="failed", error=str(e))
            raise

        logger.info("ETL refresh completed (%s mode).", mode)
        return {
            "action": "refresh",
            "mode": mode,
            "changed": True,
            "profile": json.dumps(prof.__dict__),
        }

