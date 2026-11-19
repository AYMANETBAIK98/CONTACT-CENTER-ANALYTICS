"""Inputs of the real-time analyst logic, read from the landing files (forecast, schedule)
and from the KPIs computed so far (PostgreSQL realtime.interval_kpis, or replayed from the files)."""
from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from streaming.events import INTERVAL, KpiState, load_schedule, parse_ts, split_by_interval

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"


def load_forecast(day: str, landing: Path = LANDING) -> dict[str, dict[datetime, tuple[float, float]]]:
    """market -> interval_start -> (forecast calls, forecast AHT seconds)"""
    out: dict[str, dict] = defaultdict(dict)
    for path in sorted((landing / "forecast" / f"date={day}").glob("market=*/forecast.csv")):
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                out[r["market"]][parse_ts(r["interval_start_utc"])] = (float(r["forecast_contacts"]),
                                                                      float(r["forecast_aht_seconds"]))
    return dict(out)


def load_planned_agents(day: str, landing: Path = LANDING) -> dict[str, dict[datetime, float]]:
    """market -> interval_start -> agents planned on WORK (planned work seconds / 900)."""
    plans = load_schedule(landing, day)
    out: dict[str, dict] = defaultdict(lambda: defaultdict(float))
    for agent, pieces in plans.items():
        market = agent.split("-")[0]
        for start, end, kind in pieces:
            if kind != "WORK":
                continue
            for bucket, a, b in split_by_interval(start, end):
                out[market][bucket] += (b - a).total_seconds() / INTERVAL.total_seconds()
    return {m: dict(v) for m, v in out.items()}


def replay_kpis(day: str, landing: Path = LANDING) -> dict[str, list[dict]]:
    """Recompute the interval KPIs of a whole day from the files, like the Kafka consumer does (step 2)."""
    from streaming.producer import CONTACT_TOPIC, day_events      # imported here: needs confluent_kafka
    state = KpiState()
    state.add_schedule(load_schedule(landing, day))
    for _, topic, e in day_events(day, landing):
        (state.on_contact_event if topic == CONTACT_TOPIC else state.on_agent_event)(e)
    state.advance()
    out: dict[str, list[dict]] = defaultdict(list)
    for r in state.rows():
        out[r["market"]].append(r)
    return dict(out)


def read_realtime_kpis(day: str) -> dict[str, list[dict]]:
    """The KPIs the Kafka consumer has written so far today (PostgreSQL realtime.interval_kpis)."""
    import psycopg2
    conn = psycopg2.connect(host=os.getenv("WAREHOUSE_HOST", "localhost"), port=os.getenv("WAREHOUSE_PORT", "5435"),
                            dbname=os.getenv("WAREHOUSE_DB", "contact_center"),
                            user=os.getenv("WAREHOUSE_USER", "cc"), password=os.getenv("WAREHOUSE_PASSWORD", "cc"))
    with conn, conn.cursor() as cur:
        cur.execute("""select market, interval_start, offered, answered, abandoned, answered_in_sl, aht_seconds, agents_productive
                       from realtime.interval_kpis
                       where (interval_start at time zone 'UTC')::date = %s order by 1, 2""", (day,))
        out: dict[str, list[dict]] = defaultdict(list)
        for m, start, offered, answered, abandoned, in_sl, aht, prod in cur.fetchall():
            out[m].append({"interval_start": start, "offered": offered, "answered": answered,
                           "abandoned": abandoned, "answered_in_sl": in_sl, "aht_seconds": float(aht) if aht is not None else None,
                           "agents_productive": float(prod or 0)})
    conn.close()
    return dict(out)
