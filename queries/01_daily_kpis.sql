SELECT
  order_date,
  COUNT(*) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS aov
FROM orders
WHERE status = 'delivered'
GROUP BY 1
ORDER BY 1;

