"""טבלאות מטא ב־SQLite: מקור, סכימה, ריצות, התראות."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import sqlite3

from .paths import get_paths
from .sql_loader import load_sql


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_db_path(db_path: Optional[Path] = None) -> Path:
    if db_path is not None:
        return db_path
    return get_paths().db_dir / "retail.db"


def connect(db_path: Optional[Path] = None) -> sqlite3.Connection:
    p = get_db_path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(p)


def init_meta_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(load_sql("meta_init_tables.sql"))
    conn.commit()


def upsert_source_state(
    conn: sqlite3.Connection,
    dataset: str,
    filename: str,
    size_bytes: int,
    sha256: str,
) -> None:
    init_meta_tables(conn)
    conn.execute(load_sql("meta_upsert_source_state.sql"), (dataset, filename, size_bytes, sha256, utc_now_iso()))
    conn.commit()


def get_source_state(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    init_meta_tables(conn)
    row = conn.execute(load_sql("meta_get_source_state.sql")).fetchone()
    if not row:
        return None
    return {
        "dataset": row[0],
        "filename": row[1],
        "size_bytes": row[2],
        "sha256": row[3],
        "updated_at": row[4],
    }


def add_alert(conn: sqlite3.Connection, kind: str, message: str) -> None:
    init_meta_tables(conn)
    conn.execute(load_sql("meta_add_alert.sql"), (utc_now_iso(), kind, message))
    conn.commit()


def clear_alerts(conn: sqlite3.Connection, kind: Optional[str] = None) -> None:
    init_meta_tables(conn)
    if kind is None:
        conn.execute(load_sql("meta_clear_alerts_all.sql"))
    else:
        conn.execute(load_sql("meta_clear_alerts_by_kind.sql"), (kind,))
    conn.commit()


def list_active_alerts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    init_meta_tables(conn)
    rows = conn.execute(load_sql("meta_list_active_alerts.sql")).fetchall()
    return [{"alert_id": r[0], "created_at": r[1], "kind": r[2], "message": r[3]} for r in rows]


@dataclass
class RunRecord:
    run_id: int
    started_at: str


def start_run(conn: sqlite3.Connection, mode: str) -> RunRecord:
    init_meta_tables(conn)
    started = utc_now_iso()
    cur = conn.execute(load_sql("meta_start_run.sql"), (started, mode, "running"))
    conn.commit()
    return RunRecord(run_id=int(cur.lastrowid), started_at=started)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str,
    rows_written: int = 0,
    error: Optional[str] = None,
) -> None:
    init_meta_tables(conn)
    conn.execute(load_sql("meta_finish_run.sql"), (utc_now_iso(), status, rows_written, error, run_id))
    conn.commit()


def upsert_schema_state(
    conn: sqlite3.Connection,
    *,
    columns_json: str,
    dtypes_json: str,
) -> None:
    init_meta_tables(conn)
    conn.execute(load_sql("meta_upsert_schema_state.sql"), (columns_json, dtypes_json, utc_now_iso()))
    conn.commit()


def get_last_success(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    init_meta_tables(conn)
    row = conn.execute(load_sql("meta_get_last_success.sql")).fetchone()
    if not row:
        return None
    return {"run_id": row[0], "finished_at": row[1], "mode": row[2], "rows_written": row[3]}

