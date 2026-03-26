INSERT INTO meta_schema_state (id, columns_json, dtypes_json, updated_at)
VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    columns_json=excluded.columns_json,
    dtypes_json=excluded.dtypes_json,
    updated_at=excluded.updated_at

