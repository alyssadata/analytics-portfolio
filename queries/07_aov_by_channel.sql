WITH delivered AS (
  SELECT
    o.order_id,
    o.total_amount,
    s.channel
  FROM orders o
  JOIN sessions s
    ON o.customer_id = s.customer_id
  WHERE o.status = 'delivered'
)
SELECT
  channel,
  COUNT(*) AS orders,
  AVG(total_amount) AS aov,
  SUM(total_amount) AS revenue
FROM delivered
GROUP BY 1
ORDER BY aov DESC;
