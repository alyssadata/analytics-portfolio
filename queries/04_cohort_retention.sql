WITH cohorts AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', signup_date) AS cohort_month
  FROM customers
),
cohort_sizes AS (
  SELECT
    cohort_month,
    COUNT(*) AS cohort_size
  FROM cohorts
  GROUP BY 1
),
customer_monthly_purchases AS (
  SELECT
    customer_id,
    DATE_TRUNC('month', order_date) AS order_month
  FROM orders
  WHERE status = 'delivered'
  GROUP BY 1, 2
),
cohort_activity AS (
  SELECT
    c.cohort_month,
    DATE_DIFF('month', c.cohort_month, p.order_month) AS months_since,
    COUNT(DISTINCT c.customer_id) AS active_customers
  FROM cohorts c
  JOIN customer_monthly_purchases p
    ON c.customer_id = p.customer_id
  GROUP BY 1, 2
)
SELECT
  a.cohort_month,
  a.months_since,
  s.cohort_size,
  a.active_customers,
  a.active_customers * 1.0 / s.cohort_size AS retention_rate
FROM cohort_activity a
JOIN cohort_sizes s
  ON a.cohort_month = s.cohort_month
WHERE a.months_since >= 0
ORDER BY 1, 2;
