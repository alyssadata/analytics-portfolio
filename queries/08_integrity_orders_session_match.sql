SELECT
  COUNT(*) AS orders_total,
  SUM(CASE WHEN s.session_id IS NULL THEN 1 ELSE 0 END) AS orders_missing_session,
  SUM(CASE WHEN s.session_id IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS missing_rate
FROM orders o
LEFT JOIN sessions s
  ON o.session_id = s.session_id;
