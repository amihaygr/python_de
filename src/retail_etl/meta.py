from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import sqlite3

from .paths import get_paths


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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_source_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            dataset TEXT,
            filename TEXT,
            size_bytes INTEGER,
            sha256 TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_schema_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            columns_json TEXT,
            dtypes_json TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_pipeline_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            mode TEXT,
            rows_written INTEGER,
            status TEXT,
            error TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta_alerts (
            alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            kind TEXT,
            message TEXT,
            active INTEGER DEFAULT 1
        )
        """
    )
    conn.commit()


def upsert_source_state(
    conn: sqlite3.Connection,
    dataset: str,
    filename: str,
    size_bytes: int,
    sha256: str,
) -> None:
    init_meta_tables(conn)
    conn.execute(
        """
        INSERT INTO meta_source_state (id, dataset, filename, size_bytes, sha256, updated_at)
        VALUES (1, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            dataset=excluded.dataset,
            filename=excluded.filename,
            size_bytes=excluded.size_bytes,
            sha256=excluded.sha256,
            updated_at=excluded.updated_at
        """,
        (dataset, filename, size_bytes, sha256, utc_now_iso()),
    )
    conn.commit()


def get_source_state(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    init_meta_tables(conn)
    row = conn.execute(
        "SELECT dataset, filename, size_bytes, sha256, updated_at FROM meta_source_state WHERE id = 1"
    ).fetchone()
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
    conn.execute(
        "INSERT INTO meta_alerts (created_at, kind, message, active) VALUES (?, ?, ?, 1)",
        (utc_now_iso(), kind, message),
    )
    conn.commit()


def clear_alerts(conn: sqlite3.Connection, kind: Optional[str] = None) -> None:
    init_meta_tables(conn)
    if kind is None:
        conn.execute("UPDATE meta_alerts SET active = 0 WHERE active = 1")
    else:
        conn.execute("UPDATE meta_alerts SET active = 0 WHERE active = 1 AND kind = ?", (kind,))
    conn.commit()


def list_active_alerts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    init_meta_tables(conn)
    rows = conn.execute(
        "SELECT alert_id, created_at, kind, message FROM meta_alerts WHERE active = 1 ORDER BY alert_id DESC"
    ).fetchall()
    return [{"alert_id": r[0], "created_at": r[1], "kind": r[2], "message": r[3]} for r in rows]


@dataclass
class RunRecord:
    run_id: int
    started_at: str


def start_run(conn: sqlite3.Connection, mode: str) -> RunRecord:
    init_meta_tables(conn)
    started = utc_now_iso()
    cur = conn.execute(
        "INSERT INTO meta_pipeline_runs (started_at, mode, status) VALUES (?, ?, ?)",
        (started, mode, "running"),
    )
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
    conn.execute(
        """
        UPDATE meta_pipeline_runs
        SET finished_at = ?, status = ?, rows_written = ?, error = ?
        WHERE run_id = ?
        """,
        (utc_now_iso(), status, rows_written, error, run_id),
    )
    conn.commit()


def upsert_schema_state(
    conn: sqlite3.Connection,
    *,
    columns_json: str,
    dtypes_json: str,
) -> None:
    init_meta_tables(conn)
    conn.execute(
        """
        INSERT INTO meta_schema_state (id, columns_json, dtypes_json, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            columns_json=excluded.columns_json,
            dtypes_json=excluded.dtypes_json,
            updated_at=excluded.updated_at
        """,
        (columns_json, dtypes_json, utc_now_iso()),
    )
    conn.commit()


def get_last_success(conn: sqlite3.Connection) -> Optional[dict[str, Any]]:
    init_meta_tables(conn)
    row = conn.execute(
        """
        SELECT run_id, finished_at, mode, rows_written
        FROM meta_pipeline_runs
        WHERE status = 'success'
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    return {"run_id": row[0], "finished_at": row[1], "mode": row[2], "rows_written": row[3]}

