SELECT
  DATE_TRUNC('week', o.order_date) AS week_start,
  s.channel,
  COUNT(*) AS orders,
  SUM(o.total_amount) AS revenue,
  AVG(o.total_amount) AS aov
FROM orders o
JOIN sessions s
  ON o.session_id = s.session_id
WHERE o.status = 'delivered'
GROUP BY 1, 2
ORDER BY 1, 2;
