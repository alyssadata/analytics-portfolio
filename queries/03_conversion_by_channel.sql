WITH purchases AS (
  SELECT DISTINCT session_id
  FROM events
  WHERE event_type = 'purchase'
)
SELECT
  s.channel,
  COUNT(DISTINCT s.session_id) AS total_sessions,
  COUNT(DISTINCT p.session_id) AS purchasing_sessions,
  COUNT(DISTINCT p.session_id) * 1.0 / COUNT(DISTINCT s.session_id) AS conversion_rate
FROM sessions s
LEFT JOIN purchases p
  ON s.session_id = p.session_id
GROUP BY 1
ORDER BY conversion_rate DESC;
