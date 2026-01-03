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

    Change: orders now include session_id so channel attribution is exact.
    Orders are generated only from sessions that contain a purchase event.
    """
    rng = np.random.default_rng(SEED)

    n_customers = 500
    n_sessions = 12000

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

    # Sessions
    session_customer_ids = rng.integers(1, n_customers + 1, size=n_sessions)
    sessions = pd.DataFrame(
        {
            "session_id": np.arange(1, n_sessions + 1),
            "customer_id": session_customer_ids,
            "channel": rng.choice(channels, size=n_sessions),
        }
    )

    # Events per session: always view, sometimes add_to_cart, checkout, purchase
    event_rows: list[tuple[int, int, str]] = []
    event_id = 1

    # These probabilities control funnel shape
    p_add_to_cart = 0.25
    p_checkout_given_cart = 0.60
    p_purchase_given_checkout = 0.75

    purchase_session_ids: list[int] = []

    for sid in sessions["session_id"].tolist():
        event_rows.append((event_id, sid, "view"))
        event_id += 1

        if rng.random() < p_add_to_cart:
            event_rows.append((event_id, sid, "add_to_cart"))
            event_id += 1

            if rng.random() < p_checkout_given_cart:
                event_rows.append((event_id, sid, "checkout"))
                event_id += 1

                if rng.random() < p_purchase_given_checkout:
                    event_rows.append((event_id, sid, "purchase"))
                    event_id += 1
                    purchase_session_ids.append(int(sid))

    events = pd.DataFrame(event_rows, columns=["event_id", "session_id", "event_type"])

    # Orders: only created for sessions that purchased
    if len(purchase_session_ids) == 0:
        # Extremely unlikely, but keeps the pipeline stable
        purchase_session_ids = [1]

    purchase_sessions = sessions[sessions["session_id"].isin(purchase_session_ids)].copy()

    n_orders = len(purchase_sessions)
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
    status = np.where(
        status_roll < 0.03,
        "canceled",
        np.where(status_roll < 0.06, "refunded", "delivered"),
    )

    delivery_days = (
        rng.normal(loc=5, scale=2, size=n_orders).clip(1, 20).round().astype(int)
    )
    delivered_date = [
        (d + timedelta(days=int(dd))) if s == "delivered" else None
        for d, dd, s in zip(order_dates, delivery_days, status)
    ]

    orders = pd.DataFrame(
        {
            "order_id": np.arange(1, n_orders + 1),
            "customer_id": purchase_sessions["customer_id"].to_numpy(),
            "session_id": purchase_sessions["session_id"].to_numpy(),
            "order_date": order_dates,
            "subtotal": np.round(subtotal, 2),
            "discount": np.round(discount, 2),
            "shipping_fee": np.round(shipping_fee, 2),
            "total_amount": np.round(total_amount, 2),
            "status": status,
            "delivered_date": delivered_date,
        }
    )

    return {
        "customers": customers,
        "sessions": sessions,
        "events": events,
        "orders": orders,
    }


def build_duckdb(tables: dict[str, pd.DataFrame]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))

    for name, df in tables.items():
        con.register(f"df_{name}", df)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM df_{name};")
        con.unregister(f"df_{name}")

    return con


def run_queries(con: duckdb.DuckDBPyConnection) -> list[str]:
    ran: list[str] = []
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

    def load_output_csv(stem: str) -> pd.DataFrame | None:
        p = OUTPUTS_DIR / f"{stem}.csv"
        if not p.exists():
            return None
        return pd.read_csv(p)

    daily = load_output_csv("01_daily_kpis")
    funnel = load_output_csv("02_funnel_counts")
    by_channel = load_output_csv("03_conversion_by_channel")
    cohort = load_output_csv("04_cohort_retention")
    ship = load_output_csv("05_shipping_speed_impact")
    repeat = load_output_csv("06_repeat_purchase_rate")
    aov_by_channel = load_output_csv("07_aov_by_channel")

    lines: list[str] = []
    lines.append("# Project 01 | E-commerce KPIs, funnel, and retention\n\n")
    lines.append("## Executive summary\n")
    lines.append("This report is generated automatically from SQL outputs in `outputs/`.\n\n")

    if daily is not None and not daily.empty:
        daily_sorted = daily.sort_values("order_date")
        latest = daily_sorted.iloc[-1]
        lines.append("### Headline metrics\n")
        lines.append(f"- Latest date: {latest['order_date']}\n")
        lines.append(f"- Revenue (latest day): {float(latest['revenue']):0.2f}\n")
        lines.append(f"- Orders (latest day): {int(latest['orders'])}\n")
        lines.append(f"- AOV (latest day): {float(latest['aov']):0.2f}\n\n")

    if by_channel is not None and not by_channel.empty:
        top = by_channel.sort_values("conversion_rate", ascending=False).iloc[0]
        lines.append("### Channel conversion\n")
        lines.append(
            f"- Highest converting channel: {top['channel']} (conversion {float(top['conversion_rate']):0.3f})\n\n"
        )

    if aov_by_channel is not None and not aov_by_channel.empty:
        top_aov = aov_by_channel.sort_values("aov", ascending=False).iloc[0]
        lines.append("### Channel AOV\n")
        lines.append(
            f"- Highest AOV channel: {top_aov['channel']} (AOV {float(top_aov['aov']):0.2f})\n\n"
        )

    if repeat is not None and not repeat.empty:
        r = repeat.iloc[0]
        lines.append("### Repeat purchasing\n")
        lines.append(f"- Repeat purchase rate: {float(r['repeat_purchase_rate']):0.3f}\n")
        lines.append(
            f"- Repeat customers: {int(r['repeat_customers'])} out of {int(r['customers_with_delivered_orders'])}\n\n"
        )

    if funnel is not None and not funnel.empty and "step" in funnel.columns:
        lines.append("## Funnel snapshot (sessions)\n")
        for _, row in funnel.iterrows():
            lines.append(f"- {row['step']}: {int(row['sessions'])}\n")
        lines.append("\n")

    if cohort is not None and not cohort.empty:
        lines.append("## Cohort retention\n")
        lines.append("Cohorts are grouped by signup month and tracked by months since signup.\n")
        sample = cohort.sort_values(["cohort_month", "months_since"]).head(6)
        lines.append("Sample rows:\n")
        for _, row in sample.iterrows():
            lines.append(
                f"- Cohort {row['cohort_month']}, month {int(row['months_since'])}: retention {float(row['retention_rate']):0.3f}\n"
            )
        lines.append("\n")

    if ship is not None and not ship.empty:
        lines.append("## Delivery speed impact\n")
        worst = ship.sort_values("refund_rate", ascending=False).iloc[0]
        lines.append(
            f"- Highest refund-rate bucket: {worst['delivery_bucket']} (refund_rate {float(worst['refund_rate']):0.3f})\n\n"
        )

    lines.append("## Recommendations\n")
    lines.append("1. Invest in the highest converting channel with better landing pages and lifecycle follow-ups.\n")
    lines.append("2. Improve checkout completion by segmenting funnel drop-off by channel (and device once added).\n")
    lines.append("3. Reduce delivery delays in the slowest bucket and monitor refund and cancel rate movement.\n\n")

    lines.append("## Reproducibility\n")
    lines.append(f"- Database: `{DB_PATH.as_posix()}`\n")
    lines.append("- SQL sources: `queries/`\n")
    lines.append("- Output tables: `outputs/`\n\n")

    lines.append("## Queries executed\n")
    if ran_queries:
        for q in ran_queries:
            lines.append(f"- {q}\n")
    else:
        lines.append("- None\n")

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
