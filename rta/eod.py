"""Where will each market close the day? Prints the end-of-day projection at a given time.

    # live: the KPIs the Kafka consumer is writing (run the producer + consumer of step 2 first)
    docker compose run --rm tools python -m rta.eod --date 2026-11-04 --at 12:00
    # without Kafka: replay the day from the files
    docker compose run --rm tools python -m rta.eod --date 2026-11-04 --at 12:00 --source files

--at is UTC. Add --detail to see every remaining interval (calls, agents expected / needed, expected SL).
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from rta.data import load_forecast, load_planned_agents, read_realtime_kpis, replay_kpis
from rta.projection import project, summary_line


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True)
    ap.add_argument("--at", required=True, help="HH:MM, UTC")
    ap.add_argument("--source", choices=["db", "files"], default="db")
    ap.add_argument("--detail", action="store_true")
    args = ap.parse_args()

    now = datetime.fromisoformat(f"{args.date}T{args.at}:00").replace(tzinfo=timezone.utc)
    forecast, planned = load_forecast(args.date), load_planned_agents(args.date)
    kpis = read_realtime_kpis(args.date) if args.source == "db" else replay_kpis(args.date)

    print(f"End-of-day projection, {args.date} at {args.at} UTC (target 80/20)\n")
    for market in sorted(forecast):
        p = project(market, now, kpis.get(market, []), forecast[market], planned.get(market, {}))
        print(summary_line(p))
        if args.detail:
            print("   interval  calls   AHT  agents exp / need   exp SL")
            for i in p.intervals:
                flag = "  <- short" if i.agents_gap < 0 else ""
                print(f"   {i.interval_start:%H:%M}    {i.calls:5.1f}  {i.aht_seconds:4.0f}  "
                      f"{i.agents_expected:6.1f} / {i.agents_needed:<4}  {100 * i.expected_sl:5.0f}%{flag}")
            print()


if __name__ == "__main__":
    main()
