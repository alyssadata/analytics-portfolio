# analytics-portfolio

A public, reproducible analytics portfolio focused on SQL-first analysis and Python reporting.

## Project 01 | E-commerce KPIs, funnel, and retention
This project simulates an e-commerce dataset (customers, sessions, events, orders, order_items, products) and answers common business questions using SQL and Python.

### What this demonstrates
- SQL: joins, CTEs, window functions, cohorts, KPI tables
- Python: reproducible pipeline, pandas analysis, charts, report generation
- Decision writing: clear findings and next actions

### Metrics used
- Conversion rate (session to purchase)
- AOV (average order value)
- Revenue, orders, customers
- Repeat purchase rate
- Cohort retention (monthly)

### How to run
1. Create and activate a virtual environment
2. Install dependencies
3. Run the pipeline

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/run_pipeline.py
