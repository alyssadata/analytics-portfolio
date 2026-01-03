# analytics-portfolio

A public, reproducible analytics portfolio focused on SQL-first analysis and Python reporting.

## Project 01 | E-commerce KPIs, funnel, and retention
This project simulates an e-commerce dataset (customers, sessions, events, orders, order_items, products) and answers common business questions using SQL and Python.

## What this repo is
A recruiter-friendly portfolio that demonstrates end-to-end analytics work:
question framing, SQL analysis, Python reporting, and clear recommendations.

### What this demonstrates
- SQL: joins, CTEs, window functions, cohorts, KPI tables
- Python: reproducible pipeline, pandas analysis, charts, report generation
- Decision writing: findings + next actions

### Metrics used
- Conversion rate (session to purchase)
- AOV (average order value)
- Revenue, orders, customers
- Repeat purchase rate
- Cohort retention (monthly)
- Delivery speed impact on refund and cancel rates

## Repo structure
- src/ : pipeline scripts (data generation, database build, reporting)
- queries/ : saved SQL queries
- reports/ : report output (report.md)
- data/ : generated CSVs and local database (if used)
- outputs/ : query outputs and charts
- tests/ : basic checks (optional)

## Run locally

Mac:
1) Create venv: `python3 -m venv .venv`
2) Activate venv: `source .venv/bin/activate`
3) Install deps: `python -m pip install -r requirements.txt`
4) Run pipeline: `python src/run_pipeline.py`

Windows PowerShell:
1) Create venv: `py -m venv .venv`
2) Activate venv: `.\.venv\Scripts\Activate.ps1`
3) Install deps: `py -m pip install -r requirements.txt`
4) Run pipeline: `py src/run_pipeline.py`

## Verify output
- Open: `reports/report.md`
- Confirm outputs exist in `outputs/` (CSV results and charts, if enabled)

Optional verification (if tests exist):
- Run: `pytest -q`

## Notes
- This repo uses public-safe generated data so it can be shared publicly.
- The analysis logic lives in SQL files under `queries/` and is executed by the pipeline.

## Next steps
- Add the first 5 analysis queries (daily KPIs, funnel counts, conversion by channel, cohort retention, shipping impact).
- Expand the report to include the top 3 findings and 3 recommended actions based on actual results.
