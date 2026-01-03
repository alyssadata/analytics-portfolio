WITH base AS (
  SELECT
    order_id,
    status,
    order_date,
    delivered_date,
    DATE_DIFF('day', order_date, delivered_date) AS days_to_delivery
  FROM orders
  WHERE delivered_date IS NOT NULL
),
bucketed AS (
  SELECT
    status,
    CASE
      WHEN days_to_delivery <= 3 THEN '0-3 days'
      WHEN days_to_delivery <= 6 THEN '4-6 days'
      WHEN days_to_delivery <= 10 THEN '7-10 days'
      ELSE '11+ days'
    END AS delivery_bucket
  FROM base
)
SELECT
  delivery_bucket,
  COUNT(*) AS orders,
  SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS refund_rate,
  SUM(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS cancel_rate
FROM bucketed
GROUP BY 1
ORDER BY
  CASE delivery_bucket
    WHEN '0-3 days' THEN 1
    WHEN '4-6 days' THEN 2
    WHEN '7-10 days' THEN 3
    ELSE 4
  END;
