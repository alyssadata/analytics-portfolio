from __future__ import annotations

from pathlib import Path
from datetime import date, timedelta

import numpy as np
import pandas as pd
import duckdb


ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = ROOT / "data"
OUTPUTS_DIR = ROOT / "outputs"
REPORTS_DIR = ROOT / "reports"
QUERIES_DIR = ROOT / "queries"

DB_PATH = DATA_DIR / "analytics.duckdb"

SEED = 42


def ensure_dirs() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    OUTPUTS_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)
    QUERIES_DIR.mkdir(exist_ok=True)


def generate_public_safe_tables() -> dict[str, pd.DataFrame]:
    """
    Small, reproducible e-commerce style dataset.
    Public-safe because it is synthetic.
    """
    rng = np.random.default_rng(SEED)

    n_customers = 500
    n_orders = 2500

    states = ["CA", "NY", "TX", "FL", "GA", "NC", "WA", "IL", "PA", "MA"]
    channels = ["organic", "paid_search", "paid_social", "referral", "email"]

    start = date(2024, 1, 1)
    end = date(2024, 12, 31)
    day_count = (end - start).days + 1

    customers = pd.DataFrame(
        {
            "customer_id": np.arange(1, n_customers + 1),
            "signup_date": [
                start + timedelta(days=int(x))
                for x in rng.integers(0, day_count, size=n_customers)
            ],
            "state": rng.choice(states, size=n_customers),
            "acquisition_channel": rng.choice(channels, size=n_customers),
        }
    )

    # Orders
    order_customer_ids = rng.integers(1, n_customers + 1, size=n_orders)
    order_dates = [
        start + timedelta(days=int(x))
        for x in rng.integers(0, day_count, size=n_orders)
    ]

    subtotal = rng.normal(loc=65, scale=25, size=n_orders).clip(5, 400)
    shipping_fee = rng.normal(loc=6, scale=3, size=n_orders).clip(0, 25)
    discount = rng.normal(loc=3, scale=6, size=n_orders).clip(0, 40)

    total_amount = (subtotal - discount + shipping_fee).clip(5, 500)

    # Status and delivery
    status_roll = rng.random(size=n_orders)
    status = np.where(status_roll < 0.03, "canceled", np.where(status_roll < 0.06, "refunded", "delivered"))

    delivery_days = rng.normal(loc=5, scale=2, size=n_orders).clip(1, 20).round().astype(int)
    delivered_date = [
        (d + timedelta(days=int(dd))) if s == "delivered" else None
        for d, dd, s in zip(order_dates, delivery_days, status)
    ]

    orders = pd.DataFrame(
        {
            "order_id": np.arange(1, n_orders + 1),
            "customer_id": order_customer_ids,
            "order_date": order_dates,
            "subtotal": np.round(subtotal, 2),
            "discount": np.round(discount, 2),
            "shipping_fee": np.round(shipping_fee, 2),
            "total_amount": np.round(total_amount, 2),
            "status": status,
            "delivered_date": delivered_date,
        }
    )

    # Very light sessions + events so funnel queries can exist later
    n_sessions = 4000
    session_customer_ids = rng.integers(1, n_customers + 1, size=n_sessions)

    sessions = pd.DataFrame(
        {
            "session_id": np.arange(1, n_sessions + 1),
            "customer_id": session_customer_ids,
            "channel": rng.choice(channels, size=n_sessions),
        }
    )

    # Events per session: always view, sometimes add_to_cart, checkout, purchase
    event_rows = []
    event_id = 1
    for sid in sessions["session_id"].tolist():
        event_rows.append((event_id, sid, "view"))
        event_id += 1

        if rng.random() < 0.25:
            event_rows.append((event_id, sid, "add_to_cart"))
            event_id += 1
            if rng.random() < 0.60:
                event_rows.append((event_id, sid, "checkout"))
                event_id += 1
                if rng.random() < 0.75:
                    event_rows.append((event_id, sid, "purchase"))
                    event_id += 1

    events = pd.DataFrame(event_rows, columns=["event_id", "session_id", "event_type"])

    return {
        "customers": customers,
        "orders": orders,
        "sessions": sessions,
        "events": events,
    }


def build_duckdb(tables: dict[str, pd.DataFrame]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))

    for name, df in tables.items():
        con.register(f"df_{name}", df)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df_{name};")
        con.unregister(f"df_{name}")

    return con


def run_queries(con: duckdb.DuckDBPyConnection) -> list[str]:
    ran = []
    sql_files = sorted(QUERIES_DIR.glob("*.sql"))
    for sql_path in sql_files:
        sql = sql_path.read_text(encoding="utf-8").strip()
        if not sql:
            continue

        out_name = sql_path.stem + ".csv"
        df = con.execute(sql).df()
        df.to_csv(OUTPUTS_DIR / out_name, index=False)
        ran.append(sql_path.name)

    return ran


def write_report(tables: dict[str, pd.DataFrame], ran_queries: list[str]) -> None:
    report_path = REPORTS_DIR / "report.md"

    lines: list[str] = []
    lines.append("# Project 01 | E-commerce KPIs, funnel, and retention\n\n")
    lines.append("## Pipeline status\n")
    lines.append("- Synthetic dataset generated successfully\n")
    lines.append(f"- DuckDB database created: `{DB_PATH.as_posix()}`\n\n")

    lines.append("## Table row counts\n")
    for name, df in tables.items():
        lines.append(f"- {name}: {len(df)}\n")

    lines.append("\n## Queries executed\n")
    if ran_queries:
        for q in ran_queries:
            lines.append(f"- {q}\n")
        lines.append("\nOutputs saved in `outputs/` as CSV.\n")
    else:
        lines.append("- None yet. Add `.sql` files to `queries/` and re-run the pipeline.\n")

    lines.append("\n## Next action\n")
    lines.append("Add the first SQL query: `queries/01_daily_kpis.sql` and re-run `python src/run_pipeline.py`.\n")

    report_path.write_text("".join(lines), encoding="utf-8")


def main() -> None:
    ensure_dirs()
    tables = generate_public_safe_tables()
    con = build_duckdb(tables)
    ran = run_queries(con)
    write_report(tables, ran)
    print("Done. Open reports/report.md")


if __name__ == "__main__":
    main()

