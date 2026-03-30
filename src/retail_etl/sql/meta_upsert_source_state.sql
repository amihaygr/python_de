INSERT INTO meta_source_state (id, dataset, filename, size_bytes, sha256, updated_at)
VALUES (1, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    dataset=excluded.dataset,
    filename=excluded.filename,
    size_bytes=excluded.size_bytes,
    sha256=excluded.sha256,
    updated_at=excluded.updated_at

