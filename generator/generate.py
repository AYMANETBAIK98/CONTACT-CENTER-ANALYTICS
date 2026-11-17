"""Generate simulated contact center data into data/landing/.

    docker compose run --rm tools python -m generator.generate --start 2026-11-02 --days 14

Output (one folder per dataset, partitioned by date and market, like a data lake):
    data/landing/forecast/date=2026-11-02/market=UK/forecast.csv
    data/landing/schedule/date=2026-11-02/market=UK/schedule.csv
    data/landing/contacts/date=2026-11-02/market=UK/contacts.jsonl
    data/landing/agent_events/date=2026-11-02/market=UK/agent_events.jsonl

Same --seed = same data, so results are reproducible.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from generator import config as cfg
from generator.simulate import DayData, iso, simulate_day

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"


def partition(dataset: str, day: date, market: str) -> Path:
    folder = LANDING / dataset / f"date={day.isoformat()}" / f"market={market}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def write_day(day: date, market: str, data: DayData) -> None:
    with open(partition("forecast", day, market) / "forecast.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["interval_start_utc", "market", "forecast_contacts", "forecast_aht_seconds"])
        for iv in data.forecast:
            w.writerow([iso(iv.start), market, iv.forecast_contacts, iv.forecast_aht])

    with open(partition("schedule", day, market) / "schedule.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["agent_username", "market", "activity", "start_utc", "end_utc"])
        for s in data.schedule:
            w.writerow([s.agent_id, market, "SHIFT", iso(s.start), iso(s.end)])
            for start, minutes, state in s.breaks:
                w.writerow([s.agent_id, market, state, iso(start), iso(start + timedelta(minutes=minutes))])

    for dataset, rows in (("contacts", data.contacts), ("agent_events", data.agent_events)):
        with open(partition(dataset, day, market) / f"{dataset}.jsonl", "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")


def summary(data: DayData) -> str:
    """The numbers a real-time analyst checks first, to see if the day looks realistic."""
    offered = len(data.contacts)
    answered = [c for c in data.contacts if c["Agent"]]
    in_sl = sum(1 for c in answered if c["Queue"]["Duration"] <= cfg.SERVICE_LEVEL_SECONDS)
    forecast = sum(iv.forecast_contacts for iv in data.forecast)
    asa = sum(c["Queue"]["Duration"] for c in answered) / len(answered) if answered else 0
    return (f"forecast {forecast:6.0f} | offered {offered:5d} | answered {len(answered):5d} | "
            f"abandon {1 - len(answered) / offered:6.1%} | SL {in_sl / offered:6.1%} | ASA {asa:5.0f}s | "
            f"agents {data.present_agents:3d}/{len(data.schedule):3d}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 11, 2), help="first day (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="number of days")
    parser.add_argument("--seed", type=int, default=42, help="random seed (same seed = same data)")
    args = parser.parse_args()

    started = datetime.now(timezone.utc)
    totals = {"contacts": 0, "agent_events": 0}
    for offset in range(args.days):
        day = args.start + timedelta(days=offset)
        for market in cfg.MARKETS:
            data = simulate_day(market, day, args.seed)
            if not data.forecast:
                print(f"{day} {day:%a} {market.code}  closed")
                continue
            write_day(day, market.code, data)
            totals["contacts"] += len(data.contacts)
            totals["agent_events"] += len(data.agent_events)
            print(f"{day} {day:%a} {market.code}  {summary(data)}")
    seconds = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nDone in {seconds:.0f}s: {totals['contacts']:,} contacts, {totals['agent_events']:,} agent events "
          f"written to {LANDING.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
