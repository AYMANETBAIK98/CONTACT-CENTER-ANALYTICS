"""The real-time analyst's wallboard at a given moment: adherence colour per agent and the exceptions to act on.

    docker compose run --rm tools python -m rta.wallboard --date 2026-11-04 --at 10:20
    docker compose run --rm tools python -m rta.wallboard --date 2026-11-04 --at 10:20 --market UK --all

Replays the schedule and the agent events of the day up to --at (UTC). In production the same logic
would read the agent-events topic live (step 2).
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone

from rta.adherence import agent_status
from rta.data import LANDING
from streaming.events import load_schedule

PRIORITY = {"NOT LOGGED IN": 0, "PAUSE OVERRUN": 1, "UNPLANNED PAUSE": 2, "WORKING IN PAUSE": 3, "OTHER": 4}


def statuses(day: str, now: datetime, landing=LANDING):
    plans = load_schedule(landing, day)
    events = defaultdict(list)
    for path in sorted((landing / "agent_events" / f"date={day}").glob("market=*/agent_events.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                e = json.loads(line)
                events[e["AgentUsername"]].append(e)
    return [agent_status(a, plan, events.get(a, []), now) for a, plan in sorted(plans.items())]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True)
    ap.add_argument("--at", required=True, help="HH:MM, UTC")
    ap.add_argument("--market")
    ap.add_argument("--all", action="store_true", help="list every agent, not only the exceptions")
    args = ap.parse_args()
    now = datetime.fromisoformat(f"{args.date}T{args.at}:00").replace(tzinfo=timezone.utc)
    rows = [s for s in statuses(args.date, now) if not args.market or s.market == args.market]

    print(f"Adherence wallboard, {args.date} at {args.at} UTC\n")
    for market in sorted({s.market for s in rows}):
        m = [s for s in rows if s.market == market and s.band != "Not started"]
        bands = Counter(s.band for s in m)
        on_plan = [s for s in m if s.planned_now]
        print(f"{market}: {len(on_plan)} agents planned now, {sum(not s.in_adherence for s in on_plan)} out of adherence | "
              + " ".join(f"{b} {bands.get(b, 0)}" for b in ["Green", "Amber", "Red", "Absent"]))
        shown = m if args.all else [s for s in m if s.exception]
        for s in sorted(shown, key=lambda s: (PRIORITY.get(s.exception, 9), -s.minutes_out)):
            adh = "  -  " if s.adherence is None else f"{100 * s.adherence:4.0f}%"
            what = f"{s.exception} for {s.minutes_out:.0f} min" if s.exception else "in adherence"
            print(f"   {s.agent:<8} {s.band:<6} {adh}  planned {s.planned_now or '-':<5}  doing {s.actual_now or '-':<10}  {what}")
        print()


if __name__ == "__main__":
    main()
