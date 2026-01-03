WITH delivered AS (
  SELECT customer_id
  FROM orders
  WHERE status = 'delivered'
),
orders_per_customer AS (
  SELECT
    customer_id,
    COUNT(*) AS delivered_orders
  FROM delivered
  GROUP BY 1
)
SELECT
  COUNT(*) AS customers_with_delivered_orders,
  SUM(CASE WHEN delivered_orders >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
  SUM(CASE WHEN delivered_orders >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS repeat_purchase_rate
FROM orders_per_customer;
