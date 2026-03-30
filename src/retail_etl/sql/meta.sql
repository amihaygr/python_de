-- Bundled metadata / observability SQL. Load via utils.load_sql_section("meta.sql", "<section>").

-- @section init_tables
CREATE TABLE IF NOT EXISTS meta_source_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    dataset TEXT,
    filename TEXT,
    size_bytes INTEGER,
    sha256 TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS meta_schema_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    columns_json TEXT,
    dtypes_json TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS meta_pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT,
    finished_at TEXT,
    mode TEXT,
    rows_written INTEGER,
    status TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS meta_alerts (
    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT,
    kind TEXT,
    message TEXT,
    active INTEGER DEFAULT 1
);
-- @end

-- @section upsert_source_state
INSERT INTO meta_source_state (id, dataset, filename, size_bytes, sha256, updated_at)
VALUES (1, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    dataset=excluded.dataset,
    filename=excluded.filename,
    size_bytes=excluded.size_bytes,
    sha256=excluded.sha256,
    updated_at=excluded.updated_at
-- @end

-- @section get_source_state
SELECT dataset, filename, size_bytes, sha256, updated_at
FROM meta_source_state
WHERE id = 1
-- @end

-- @section add_alert
INSERT INTO meta_alerts (created_at, kind, message, active)
VALUES (?, ?, ?, 1)
-- @end

-- @section clear_alerts_all
UPDATE meta_alerts
SET active = 0
WHERE active = 1
-- @end

-- @section clear_alerts_by_kind
UPDATE meta_alerts
SET active = 0
WHERE active = 1
  AND kind = ?
-- @end

-- @section list_active_alerts
SELECT alert_id, created_at, kind, message
FROM meta_alerts
WHERE active = 1
ORDER BY alert_id DESC
-- @end

-- @section start_run
INSERT INTO meta_pipeline_runs (started_at, mode, status)
VALUES (?, ?, ?)
-- @end

-- @section finish_run
UPDATE meta_pipeline_runs
SET finished_at = ?,
    status = ?,
    rows_written = ?,
    error = ?
WHERE run_id = ?
-- @end

-- @section upsert_schema_state
INSERT INTO meta_schema_state (id, columns_json, dtypes_json, updated_at)
VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    columns_json=excluded.columns_json,
    dtypes_json=excluded.dtypes_json,
    updated_at=excluded.updated_at
-- @end

-- @section get_last_success
SELECT run_id, finished_at, mode, rows_written
FROM meta_pipeline_runs
WHERE status = 'success'
ORDER BY run_id DESC
LIMIT 1
-- @end
