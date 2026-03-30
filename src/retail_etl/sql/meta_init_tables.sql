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

