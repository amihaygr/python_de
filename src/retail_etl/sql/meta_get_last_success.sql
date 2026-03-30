SELECT run_id, finished_at, mode, rows_written
FROM meta_pipeline_runs
WHERE status = 'success'
ORDER BY run_id DESC
LIMIT 1

