SELECT
  event_type AS step,
  COUNT(*) AS events,
  COUNT(DISTINCT session_id) AS sessions
FROM events
GROUP BY 1
ORDER BY
  CASE event_type
    WHEN 'view' THEN 1
    WHEN 'add_to_cart' THEN 2
    WHEN 'checkout' THEN 3
    WHEN 'purchase' THEN 4
    ELSE 99
  END;
