"""How good is the projection? Replays every simulated day, projects the end-of-day SL at fixed times,
and compares it with the SL the day really closed at.

    docker compose run --rm tools python -m rta.backtest

A projection a real-time analyst cannot trust is worse than none: this is the number that says how much to trust it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from statistics import mean

from rta.data import LANDING, load_forecast, load_planned_agents, replay_kpis
from rta.projection import project

CHECKPOINTS = ["10:00", "12:00", "14:00", "16:00"]       # UTC


def run(days: list[str]) -> list[dict]:
    results = []
    for day in days:
        forecast, planned, kpis = load_forecast(day), load_planned_agents(day), replay_kpis(day)
        for market, rows in kpis.items():
            offered = sum(r["offered"] for r in rows)
            if not offered:
                continue
            actual = sum(r["answered_in_sl"] for r in rows) / offered
            for at in CHECKPOINTS:
                now = datetime.fromisoformat(f"{day}T{at}:00").replace(tzinfo=timezone.utc)
                p = project(market, now, rows, forecast[market], planned.get(market, {}))
                if p.projected_sl is None or p.offered_so_far == 0 or p.calls_remaining == 0:
                    continue                    # market not open yet, or already closed
                naive = p.sl_so_far             # "the day will end like it is now"
                results.append({"day": day, "market": market, "at": at, "actual": actual,
                                "projected": p.projected_sl, "naive": naive, "status": p.status})
    return results


def main() -> None:
    days = sorted(p.name.split("=")[1] for p in (LANDING / "contacts").glob("date=*"))
    res = run(days)
    print(f"{len(res)} projections over {len(days)} days x 3 markets\n")
    print("checkpoint   projection error   'SL so far' error   (mean absolute, SL points)")
    for at in CHECKPOINTS:
        r = [x for x in res if x["at"] == at]
        if r:
            print(f"{at} UTC      {100 * mean(abs(x['projected'] - x['actual']) for x in r):6.1f}"
                  f"             {100 * mean(abs(x['naive'] - x['actual']) for x in r):6.1f}       (n={len(r)})")
    hits = [x for x in res if (x["projected"] >= 0.8) == (x["actual"] >= 0.8)]
    print(f"\nRight call on 'will we close above 80 %?': {len(hits)}/{len(res)} ({100 * len(hits) / len(res):.0f} %)")


if __name__ == "__main__":
    main()
