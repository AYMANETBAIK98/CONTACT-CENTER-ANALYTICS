"""AUTOMATIC INTRADAY STATUS MESSAGE: the update a real-time analyst sends to team managers, ops and WFM
several times a day, written from the projection (5a) and the adherence wallboard (5b).

    docker compose run --rm tools python -m rta.status_message --date 2026-11-04 --at 12:00 --source files

Plain text on purpose: it is pasted as-is into Teams, Slack or an email. It is also saved in reports/
(not in Git). Times are UTC; only CLOSED intervals count ("closed intervals up to 12:00").

Probability of closing on target: the projection error measured by the backtest (rta.backtest) shrinks as the day
goes on, so the uncertainty is modelled as 0.5 point + 4 points x the share of the day's calls still to come
(about 3.5 points at 10:00, under 1.5 after 16:00), and the probability is the normal-curve chance that the
real close lands above 80 % given the projection.
"""
from __future__ import annotations

import argparse
import math
import os
from datetime import datetime, timezone
from pathlib import Path

from rta.adherence import AgentStatus
from rta.data import LANDING, ROOT, load_forecast, load_planned_agents, read_realtime_kpis, replay_kpis
from rta.projection import SL_TARGET, Projection, project
from rta.wallboard import statuses

FLAGS = {"UK": "🇬🇧", "DE": "🇩🇪", "ES": "🇪🇸"}
NAMES = {"UK": "UK", "DE": "DE", "ES": "ES"}
REPORTS = Path(os.getenv("REPORTS_DIR", ROOT / "reports"))


def probability_on_target(p: Projection) -> float:
    total = p.offered_so_far + p.calls_remaining
    if p.projected_sl is None or not total:
        return 0.0
    sigma = 0.005 + 0.04 * p.calls_remaining / total
    z = (p.projected_sl - SL_TARGET) / sigma
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def risk(prob: float) -> str:
    return "🟢 LOW" if prob >= 0.9 else "🟡 MEDIUM" if prob >= 0.6 else "🔴 HIGH"


def dot(x: float | None, green: float = SL_TARGET) -> str:
    return "⚪" if x is None else "🟢" if x >= green else "🔴"


def pct(x: float | None) -> str:
    return "-" if x is None else f"{100 * x:.0f}%"


def market_block(market: str, p: Projection, rows: list[dict], agents: list[AgentStatus]) -> list[str]:
    closed = [r for r in rows if r["interval_start"] < p.now and r["offered"]]
    abandoned = sum(r.get("abandoned") or 0 for r in closed)
    prob = probability_on_target(p)
    lines = [f"{FLAGS[market]} {NAMES[market]}",
             f"Global: SL {dot(p.sl_so_far)} {pct(p.sl_so_far)} | EOD Projection {pct(p.projected_sl)} | "
             f"{100 * prob:.0f}% Probability | {risk(prob)} Risk"]
    if not closed:
        return lines + ["* Not open yet."]

    vol = p.volume_factor - 1
    lines.append(f"* Day so far: {p.offered_so_far} offered, {pct(abandoned / p.offered_so_far)} abandoned, "
                 f"volume {'+' if vol >= 0 else ''}{100 * vol:.0f}% vs forecast.")
    worst = min(closed, key=lambda r: r["answered_in_sl"] / r["offered"])
    worst_sl = worst["answered_in_sl"] / worst["offered"]
    if worst_sl < SL_TARGET:
        lines.append(f"* Lowest interval: {worst['interval_start']:%H:%M} at {pct(worst_sl)} "
                     f"({worst['offered']} offered).")
    short = sorted((i for i in p.intervals if i.agents_gap <= -1), key=lambda i: i.agents_gap)[:3]
    if p.status == "CLOSED":
        lines.append("* Day closed.")
    else:
        later = f"* Rest of day: {pct(p.required_sl)} SL needed from now to close at {pct(SL_TARGET)}"
        if short:
            later += "; short: " + ", ".join(f"{i.interval_start:%H:%M} ({i.agents_gap:+.0f})" for i in sorted(short, key=lambda i: i.interval_start))
        lines.append(later + ".")

    planned = [a for a in agents if a.band not in ("Not started",)]
    bands = {b: sum(a.band == b for a in planned) for b in ("Green", "Amber", "Red", "Absent")}
    not_in = [a for a in agents if a.exception == "NOT LOGGED IN" and a.band != "Absent"]
    overrun = [a for a in agents if a.exception in ("PAUSE OVERRUN", "UNPLANNED PAUSE") and a.minutes_out >= 3]
    lines.append(f"* Adherence: 🟢 {bands['Green']} | 🟡 {bands['Amber']} | 🔴 {bands['Red']} | "
                 f"absent {bands['Absent']}" + (f" | {len(overrun)} in pause past schedule" if overrun else "") + ".")

    actions = []
    if overrun:
        actions.append("back from pause: " + ", ".join(f"{a.agent} ({a.minutes_out:.0f} min)"
                                                       for a in sorted(overrun, key=lambda a: -a.minutes_out)[:4]))
    if not_in:
        actions.append("not logged in: " + ", ".join(a.agent for a in not_in[:4]))
    if short:
        actions.append(f"cover {short[0].interval_start:%H:%M} (moving breaks / overtime)")
    if actions:
        lines.append("RTM priority – Team Managers/Ops/WFM: " + "; ".join(actions) + ".")

    priority = {"SECURED": "Target secured: keep adherence, no extra action needed.",
                "ON TRACK": "Priority is to protect the projection: keep breaks on time.",
                "AT RISK": f"Priority is service level: {pct(p.required_sl)} needed for the rest of the day.",
                "TARGET LOST": "Target out of reach today: priority is to limit abandons and protect tomorrow.",
                "CLOSED": "Day closed."}[p.status]
    lines.append(priority)
    return lines


def build_message(day: str, at: str, source: str = "files", landing: Path = LANDING) -> str:
    now = datetime.fromisoformat(f"{day}T{at}:00").replace(tzinfo=timezone.utc)
    forecast, planned = load_forecast(day, landing), load_planned_agents(day, landing)
    kpis = read_realtime_kpis(day) if source == "db" else replay_kpis(day, landing)
    wall = statuses(day, now, landing)
    out = [f"RTM UPDATE – {day} (closed intervals up to {at} UTC)", ""]
    for market in ("UK", "DE", "ES"):
        if market not in forecast:
            continue
        rows = kpis.get(market, [])
        p = project(market, now, rows, forecast[market], planned.get(market, {}))
        out += market_block(market, p, rows, [a for a in wall if a.market == market]) + [""]
    out.append("Simulated data · generated by rta/status_message.py")
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True)
    ap.add_argument("--at", required=True, help="HH:MM, UTC")
    ap.add_argument("--source", choices=["db", "files"], default="db")
    args = ap.parse_args()
    text = build_message(args.date, args.at, args.source)
    print(text)
    REPORTS.mkdir(exist_ok=True)
    path = REPORTS / f"rtm_update_{args.date}_{args.at.replace(':', '')}.txt"
    path.write_text(text + "\n", encoding="utf-8")
    print(f"\n(saved to {path})")


if __name__ == "__main__":
    main()
