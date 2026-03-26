UPDATE meta_pipeline_runs
SET finished_at = ?,
    status = ?,
    rows_written = ?,
    error = ?
WHERE run_id = ?

