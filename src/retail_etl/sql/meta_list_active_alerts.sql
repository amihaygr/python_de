SELECT alert_id, created_at, kind, message
FROM meta_alerts
WHERE active = 1
ORDER BY alert_id DESC

