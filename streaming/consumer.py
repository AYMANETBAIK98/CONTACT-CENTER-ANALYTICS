"""CONSUMER: reads both topics and keeps real-time KPIs per market and 15-minute interval
in PostgreSQL (table realtime.interval_kpis), like the live view of a real-time analyst.

    docker compose run --rm tools python -m streaming.consumer

Start it in one terminal, then start the producer in another one.

Design choices (each one answers "what if it crashes or runs twice?"):
  - It reads the topics from the beginning at every start and rebuilds its state from the log.
    Kafka keeps the events, so the state can always be recomputed: no state is lost on a restart.
  - Each event has an id; an event already counted is ignored (a replayed day is not counted twice).
  - Rows are written with an UPSERT on (market, interval_start): writing the same interval again
    replaces it, it never adds a duplicate row.
"""
from __future__ import annotations

import json
import os
import signal
import time
import uuid
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

from streaming.events import INTERVAL, KpiState, interval_start, load_schedule

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"
TOPICS = ["contact-events", "agent-events"]
FLUSH_EVERY_SECONDS = 2

DDL = """
CREATE SCHEMA IF NOT EXISTS realtime;
CREATE TABLE IF NOT EXISTS realtime.interval_kpis (
    market            TEXT        NOT NULL,
    interval_start    TIMESTAMPTZ NOT NULL,
    offered           INT         NOT NULL,
    answered          INT         NOT NULL,
    abandoned         INT         NOT NULL,
    answered_in_sl    INT         NOT NULL,
    service_level     NUMERIC(5,4),          -- answered within 20 s / offered
    abandon_rate      NUMERIC(5,4),
    asa_seconds       NUMERIC(8,1),          -- average speed of answer
    aht_seconds       NUMERIC(8,1),          -- average handle time (talk + wrap-up)
    occupancy         NUMERIC(5,4),          -- busy time / productive time
    adherence         NUMERIC(5,4),          -- time following the schedule / scheduled time
    agents_productive NUMERIC(6,2),          -- average agents logged in and productive
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (market, interval_start)
);
"""

UPSERT = """
INSERT INTO realtime.interval_kpis (market, interval_start, offered, answered, abandoned, answered_in_sl,
    service_level, abandon_rate, asa_seconds, aht_seconds, occupancy, adherence, agents_productive)
VALUES %s
ON CONFLICT (market, interval_start) DO UPDATE SET
    offered = EXCLUDED.offered, answered = EXCLUDED.answered, abandoned = EXCLUDED.abandoned,
    answered_in_sl = EXCLUDED.answered_in_sl, service_level = EXCLUDED.service_level,
    abandon_rate = EXCLUDED.abandon_rate, asa_seconds = EXCLUDED.asa_seconds,
    aht_seconds = EXCLUDED.aht_seconds, occupancy = EXCLUDED.occupancy, adherence = EXCLUDED.adherence,
    agents_productive = EXCLUDED.agents_productive, updated_at = now()
"""
COLUMNS = ["market", "interval_start", "offered", "answered", "abandoned", "answered_in_sl", "service_level",
           "abandon_rate", "asa_seconds", "aht_seconds", "occupancy", "adherence", "agents_productive"]


def dsn() -> str:
    return (f"host={os.getenv('WAREHOUSE_HOST', 'localhost')} port={os.getenv('WAREHOUSE_PORT', '5435')} "
            f"dbname={os.getenv('WAREHOUSE_DB', 'contact_center')} user={os.getenv('WAREHOUSE_USER', 'cc')} "
            f"password={os.getenv('WAREHOUSE_PASSWORD', 'cc')}")


def write(conn, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, UPSERT, [tuple(r[c] for c in COLUMNS) for r in rows])
    conn.commit()


def pct(value) -> str:
    return "  -  " if value is None else f"{value:5.0%}"


def status_line(state: KpiState) -> str:
    """The last closed interval of each market, the way an RTA reads it."""
    last = interval_start(state.clock) - INTERVAL
    parts = []
    for market in ("UK", "DE", "ES"):
        r = state.rows([(market, last)])[0] if (market, last) in state.kpis else None
        if r and r["offered"]:
            flag = "OK " if r["service_level"] >= 0.8 else "LOW"
            parts.append(f"{market} {flag} SL{pct(r['service_level'])} abd{pct(r['abandon_rate'])} "
                         f"occ{pct(r['occupancy'])} adh{pct(r['adherence'])}")
    local = last.strftime("%H:%M")
    return f"[{local} UTC] " + " | ".join(parts) if parts else ""


def main() -> None:
    conn = psycopg2.connect(dsn())
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BROKERS", "localhost:19092"),
        "group.id": f"kpi-builder-{uuid.uuid4().hex[:8]}",   # new group = read the topics from the start
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(TOPICS)
    state = KpiState()
    loaded_days: set[str] = set()
    stop = {"now": False}                              # Ctrl+C: finish cleanly (last write, close)
    signal.signal(signal.SIGINT, lambda *_: stop.update(now=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(now=True))

    print("Waiting for events on contact-events / agent-events (Ctrl+C to stop)...")
    last_flush, last_line, events = time.monotonic(), "", 0
    while not stop["now"]:
        msg = consumer.poll(0.5)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")
            else:
                event = json.loads(msg.value())
                ts = event.get("event_ts") or event["EventTimestamp"]
                day = ts[:10]
                if day not in loaded_days:             # first event of a day: load its schedule
                    state.add_schedule(load_schedule(LANDING, day))
                    loaded_days.add(day)
                if msg.topic() == "contact-events":
                    state.on_contact_event(event)
                else:
                    state.on_agent_event(event)
                events += 1
        if time.monotonic() - last_flush >= FLUSH_EVERY_SECONDS:
            write(conn, state.take_dirty())
            last_flush = time.monotonic()
            line = status_line(state)
            if line and line != last_line:
                print(line)
                last_line = line
    write(conn, state.take_dirty())
    consumer.close()
    conn.close()
    print(f"Stopped after {events:,} events.")


if __name__ == "__main__":
    main()
