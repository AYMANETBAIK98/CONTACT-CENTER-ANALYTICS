"""LIVE SCHEDULE ADHERENCE, agent by agent: the real-time analyst's wallboard.

For every scheduled agent, at a given moment ("now"):
  - what the schedule says now (WORK, BREAK, LUNCH) and what the agent is doing (AVAILABLE, ON_CONTACT, BREAK...)
  - for how long they have been out of adherence, without interruption
  - the exception a real-time analyst acts on:
        NOT LOGGED IN     planned on WORK, not logged in (late, absent, or logged out early)
        PAUSE OVERRUN     planned back on WORK, still in the break or lunch that just ended
        UNPLANNED PAUSE   planned on WORK, in a pause the schedule does not have now
        WORKING IN PAUSE  planned on a pause, still taking contacts (lower priority, but it shifts the plan)
  - adherence so far today = time doing what the schedule says / scheduled time so far,
    and its colour: Green >= 90 %, Amber >= 85 %, Red below (same bands as dbt fct_agent_day)

Same definition of adherence as the streaming KPIs (step 2): WORK is adherent when the agent is AVAILABLE,
ON_CONTACT or in ACW; a BREAK or LUNCH is adherent when the agent is in a pause.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from streaming.events import PAUSES, PRODUCTIVE, parse_ts

GREEN, AMBER = 0.90, 0.85
ORDER = {"LOGIN": 0, "LOGOUT": 2}             # same-millisecond ties: LOGIN first, LOGOUT last (as in silver)


@dataclass
class AgentStatus:
    agent: str
    market: str
    planned_now: str | None                  # WORK / BREAK / LUNCH / None (off shift)
    actual_now: str | None                   # agent state, None = not logged in
    in_adherence: bool
    minutes_out: float                       # minutes out of adherence without interruption, up to now
    exception: str | None
    scheduled_minutes: float                 # scheduled so far today
    adherence: float | None                  # adherent / scheduled, so far
    band: str                                # Green / Amber / Red / Absent / Not started


def adherent(planned: str | None, actual: str | None) -> bool:
    return (planned == "WORK" and actual in PRODUCTIVE) or (planned in PAUSES and actual in PAUSES)


def at(timeline: list[tuple[datetime, datetime, str]], t: datetime) -> str | None:
    for start, end, value in timeline:
        if start <= t < end:
            return value
    return None


def state_timeline(events: list[dict], now: datetime) -> list[tuple[datetime, datetime, str]]:
    """Agent events -> [(start, end, state)]: each state lasts until the next event (the last one until now)."""
    evs = sorted(((parse_ts(e["EventTimestamp"]), ORDER.get(e["State"], 1), e["State"]) for e in events), key=lambda x: x[:2])
    evs = [e for e in evs if e[0] <= now]
    out = []
    for (t0, _, state), nxt in zip(evs, evs[1:] + [(now, 0, None)]):
        if state != "LOGOUT" and nxt[0] > t0:
            out.append((t0, nxt[0], state))
    return out


def agent_status(agent: str, plan: list[tuple[datetime, datetime, str]], events: list[dict],
                 now: datetime) -> AgentStatus:
    market = agent.split("-")[0]
    actual = state_timeline(events, now)
    shift_start = min(p[0] for p in plan)
    if now <= shift_start:
        return AgentStatus(agent, market, None, None, True, 0, None, 0, None, "Not started")

    # cut the day at every change of plan or state, and judge each piece
    cuts = sorted({t for p in plan for t in p[:2] if t < now} | {t for a in actual for t in a[:2] if t < now} | {now})
    pieces = []                               # (start, end, planned, actual, adherent)
    for t0, t1 in zip(cuts, cuts[1:]):
        planned, state = at(plan, t0), at(actual, t0)
        pieces.append((t0, t1, planned, state, adherent(planned, state)))
    scheduled = sum((p[1] - p[0]).total_seconds() for p in pieces if p[2] is not None)
    good = sum((p[1] - p[0]).total_seconds() for p in pieces if p[2] is not None and p[4])

    # out of adherence right now: add up the pieces from now backwards while they are out
    minutes_out = 0.0
    for t0, t1, planned, state, ok in reversed(pieces):
        if planned is None or ok:
            break
        minutes_out += (t1 - t0).total_seconds() / 60

    planned_now = at(plan, now)
    actual_now = actual[-1][2] if actual and actual[-1][1] == now else None   # the state still running now
    exception = None
    if planned_now is not None and not adherent(planned_now, actual_now):
        if planned_now == "WORK" and actual_now is None:
            exception = "NOT LOGGED IN"
        elif planned_now == "WORK" and actual_now in PAUSES:
            # overrun = the pause the agent is in started before the planned pause ended
            last_pause_end = max((p[1] for p in plan if p[2] in PAUSES and p[1] <= now), default=None)
            pause_started = actual[-1][0]
            overrun = last_pause_end is not None and pause_started < last_pause_end
            exception = "PAUSE OVERRUN" if overrun else "UNPLANNED PAUSE"
        elif planned_now in PAUSES and actual_now in PRODUCTIVE:
            exception = "WORKING IN PAUSE"
        else:
            exception = "OTHER"

    adherence = good / scheduled if scheduled else None
    if not actual:
        band = "Absent"
    elif adherence is None:
        band = "Not started"
    else:
        band = "Green" if adherence >= GREEN else "Amber" if adherence >= AMBER else "Red"
    return AgentStatus(agent, market, planned_now, actual_now, exception is None, round(minutes_out, 1),
                       exception, round(scheduled / 60, 1), adherence, band)
