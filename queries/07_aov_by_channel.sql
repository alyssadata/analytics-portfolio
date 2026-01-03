WITH delivered AS (
  SELECT
    o.order_id,
    o.customer_id,
    o.session_id,
    o.order_date,
    o.total_amount
  FROM orders o
  WHERE o.status = 'delivered'
),
joined AS (
  SELECT
    d.order_id,
    d.customer_id,
    d.session_id,
    d.order_date,
    d.total_amount,
    s.channel
  FROM delivered d
  JOIN sessions s
    ON d.session_id = s.session_id
)
SELECT
  channel,
  COUNT(*) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  AVG(total_amount) AS aov,
  SUM(total_amount) AS revenue,
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS last_order_date
FROM joined
GROUP BY 1
ORDER BY aov DESC;

