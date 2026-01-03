# Project 01 | E-commerce KPIs, funnel, and retention

## Executive summary
This report is generated automatically from SQL outputs in `outputs/`.

### Headline metrics
- Latest date: 2024-12-31
- Revenue (latest day): 237.98
- Orders (latest day): 4
- AOV (latest day): 59.50

### Channel conversion
- Highest converting channel: paid_social (conversion 0.112)

### Channel AOV
- Highest AOV channel: organic (AOV 68.00)

### Repeat purchasing
- Repeat purchase rate: 0.751
- Repeat customers: 344 out of 458

## Funnel snapshot (sessions)
- view: 12000
- add_to_cart: 2977
- checkout: 1753
- purchase: 1290

## Cohort retention
Cohorts are grouped by signup month and tracked by months since signup.
Sample rows:
- Cohort 2024-01-01, month 0: retention 0.143
- Cohort 2024-01-01, month 1: retention 0.143
- Cohort 2024-01-01, month 2: retention 0.200
- Cohort 2024-01-01, month 3: retention 0.171
- Cohort 2024-01-01, month 4: retention 0.143
- Cohort 2024-01-01, month 5: retention 0.229

## Delivery speed impact
- Highest refund-rate bucket: 0-3 days (refund_rate 0.000)

## Recommendations
1. Invest in the highest converting channel with better landing pages and lifecycle follow-ups.
2. Improve checkout completion by segmenting funnel drop-off by channel (and device once added).
3. Reduce delivery delays in the slowest bucket and monitor refund and cancel rate movement.

## Reproducibility
- Database: `/home/runner/work/analytics-portfolio/analytics-portfolio/data/analytics.duckdb`
- SQL sources: `queries/`
- Output tables: `outputs/`

## Queries executed
- 01_daily_kpis.sql
- 02_funnel_counts.sql
- 03_conversion_by_channel.sql
- 04_cohort_retention.sql
- 05_shipping_speed_impact.sql
- 06_repeat_purchase_rate.sql
- 07_aov_by_channel.sql
