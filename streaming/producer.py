"""PRODUCER: replays one simulated day into Kafka, in time order, as if it were happening live.

    docker compose run --rm tools python -m streaming.producer --date 2026-11-04 --speed 120

--speed 120 = 2 hours of the day pass in 1 real minute (a 12-hour day in ~6 minutes).
--speed 0   = no waiting, everything is sent as fast as possible.

Topics:
  contact-events  QUEUED / ANSWERED / ABANDONED / ENDED events, built from the contact records
  agent-events    agent state changes (LOGIN, AVAILABLE, ON_CONTACT, ACW, BREAK, LUNCH, LOGOUT)
The message key is the market: Kafka keeps the order of messages that share a key,
so the events of one market always arrive in the order they happened.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer

from streaming.events import contact_lifecycle_events, parse_ts

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"
CONTACT_TOPIC, AGENT_TOPIC = "contact-events", "agent-events"


def read_jsonl(paths):
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)


def day_events(day: str, landing: Path = LANDING) -> list[tuple[str, str, dict]]:
    """All events of the day, every market, sorted by time: (timestamp, topic, payload)."""
    contacts = sorted((landing / "contacts" / f"date={day}").glob("market=*/contacts.jsonl"))
    agents = sorted((landing / "agent_events" / f"date={day}").glob("market=*/agent_events.jsonl"))
    if not contacts:
        raise SystemExit(f"No data for {day} in {landing}. Run the generator first (step 1).")
    events = [(e["event_ts"], CONTACT_TOPIC, e) for ctr in read_jsonl(contacts) for e in contact_lifecycle_events(ctr)]
    events += [(e["EventTimestamp"], AGENT_TOPIC, e) for e in read_jsonl(agents)]
    events.sort(key=lambda e: (e[0], e[1]))           # time order (topic name only breaks ties)
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default="2026-11-04", help="day to replay (YYYY-MM-DD)")
    parser.add_argument("--speed", type=float, default=120, help="simulated seconds per real second (0 = no wait)")
    args = parser.parse_args()

    events = day_events(args.date)
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BROKERS", "localhost:19092"),
                         "linger.ms": 50, "acks": "all"})
    first = parse_ts(events[0][0])
    started = time.monotonic()
    print(f"Replaying {len(events):,} events of {args.date} at x{args.speed:g} "
          f"({'no wait' if not args.speed else f'{(parse_ts(events[-1][0]) - first).total_seconds() / args.speed / 60:.1f} min'})")

    for n, (ts, topic, payload) in enumerate(events, start=1):
        if args.speed:
            # wait until this event's moment in the accelerated day
            due = (parse_ts(ts) - first).total_seconds() / args.speed
            delay = due - (time.monotonic() - started)
            if delay > 0:
                time.sleep(delay)
        market = payload.get("market") or payload["Market"]
        while True:
            try:
                producer.produce(topic, key=market, value=json.dumps(payload))
                break
            except BufferError:                # local buffer full (only at --speed 0): let it drain
                producer.poll(1)
        producer.poll(0)                       # serve delivery callbacks, keep the buffer flowing
        if n % 5000 == 0:
            print(f"  {n:,} events sent, simulated time {ts[11:16]} UTC")
    producer.flush()
    print(f"Done: {len(events):,} events sent to {CONTACT_TOPIC} / {AGENT_TOPIC}")


if __name__ == "__main__":
    main()
