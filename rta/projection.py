"""END-OF-DAY SERVICE LEVEL PROJECTION: the question a real-time analyst answers all day.

    "It is 12:00. The service level so far is 84 %. Where will we end the day?
     And what SL do we need from now on to still close at 80 %?"

How it works, for one market and one moment ("now"):
  1. What already happened: the closed intervals (offered, answered within 20 s, agents productive, AHT).
  2. Re-forecast the rest of the day: today the volume runs X % above / below forecast,
     so the remaining forecast is scaled by the same factor (same for handle time).
  3. Real staffing: planned agents x the share of them actually productive today
     (absences, late logins, long breaks: the "shrinkage" seen so far).
  4. Erlang C gives the expected SL of every remaining interval with those agents and that volume,
     calibrated on today: Erlang C assumes nobody hangs up, so it is too pessimistic; the closed intervals
     show by how much (calls really missed / calls Erlang C would have missed).
  5. Projected end-of-day SL = (answered in SL so far + expected in SL later) / (offered so far + expected later).
  6. Required SL for the rest of the day to close on target, and the agents each interval is missing.

Everything here is pure Python on plain dicts, so it is tested without a database (tests/test_rta.py).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime

from generator.erlang import agents_needed, service_level

SL_SECONDS = 20
SL_TARGET = 0.80
MIN_CLOSED_FOR_REFORECAST = 4          # under 1 hour of history, trust the forecast
FACTOR_RANGE = (0.5, 1.5)              # a re-forecast never more than halves or doubles the forecast


@dataclass
class IntervalPlan:
    interval_start: datetime
    calls: float                       # re-forecast calls
    aht_seconds: float
    agents_expected: float             # planned agents x productive share
    expected_sl: float                 # Erlang C with those agents
    agents_needed: int                 # Erlang C agents for 80/20
    agents_gap: float                  # expected - needed (negative = short)


@dataclass
class Projection:
    market: str
    now: datetime
    offered_so_far: int
    in_sl_so_far: int
    sl_so_far: float | None
    volume_factor: float               # actual / forecast volume so far
    aht_factor: float                  # actual / forecast AHT so far
    productive_share: float            # agents productive / agents planned so far
    miss_factor: float                 # calls really missed / calls Erlang C would have missed, so far
    calls_remaining: float
    projected_sl: float | None         # end of day
    required_sl: float | None          # SL needed from now on to close on target (None = nothing left)
    status: str                        # ON TRACK / AT RISK / TARGET LOST / SECURED / CLOSED
    intervals: list[IntervalPlan] = field(default_factory=list)


def clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def erlang_sl(agents: float, calls: float, aht: float) -> float:
    """Erlang C with a fractional number of agents: interpolate between the two whole numbers around it."""
    lo = math.floor(agents)
    sl_lo = service_level(lo, calls, aht, SL_SECONDS) if lo > 0 else (1.0 if calls <= 0 else 0.0)
    sl_hi = service_level(lo + 1, calls, aht, SL_SECONDS)
    return sl_lo + (sl_hi - sl_lo) * (agents - lo)


def project(market: str, now: datetime, closed: list[dict], forecast: dict[datetime, tuple[float, float]],
            planned: dict[datetime, float], sl_target: float = SL_TARGET) -> Projection:
    """closed:   intervals that ended before `now`: {interval_start, offered, answered, answered_in_sl,
                 aht_seconds, agents_productive}
    forecast: interval_start -> (forecast calls, forecast AHT seconds), for the whole day
    planned:  interval_start -> agents planned on WORK (scheduled seconds / 900), for the whole day"""
    closed = [c for c in closed if c["interval_start"] < now]
    offered = sum(c["offered"] for c in closed)
    in_sl = sum(c["answered_in_sl"] for c in closed)

    # 2. re-forecast: how far is today from the forecast?
    fc_so_far = sum(forecast.get(c["interval_start"], (0, 0))[0] for c in closed)
    volume_factor = 1.0
    if len(closed) >= MIN_CLOSED_FOR_REFORECAST and fc_so_far > 0:
        volume_factor = clip(offered / fc_so_far, *FACTOR_RANGE)
    answered = [c for c in closed if c.get("answered") and c.get("aht_seconds")]
    aht_factor = 1.0
    if len(answered) >= MIN_CLOSED_FOR_REFORECAST:
        actual = sum(c["aht_seconds"] * c["answered"] for c in answered) / sum(c["answered"] for c in answered)
        planned_aht = (sum(forecast[c["interval_start"]][1] * c["answered"] for c in answered if c["interval_start"] in forecast)
                       / sum(c["answered"] for c in answered))
        aht_factor = clip(actual / planned_aht, *FACTOR_RANGE) if planned_aht else 1.0

    # 3. shrinkage seen today: how many of the planned agents were really productive
    with_plan = [c for c in closed if planned.get(c["interval_start"], 0) > 0]
    productive_share = 1.0
    if len(with_plan) >= MIN_CLOSED_FOR_REFORECAST:
        productive_share = clip(sum(c["agents_productive"] for c in with_plan)
                                / sum(planned[c["interval_start"]] for c in with_plan), 0.3, 1.2)

    # 3b. calibrate Erlang C on today: it assumes nobody hangs up, so it over-estimates the queue.
    #     Compare the calls it would have missed in the closed intervals with the calls really missed.
    miss_factor = 1.0
    scored = [c for c in closed if c["offered"] > 0 and c.get("aht_seconds")]
    if len(scored) >= MIN_CLOSED_FOR_REFORECAST:
        expected_miss = sum(c["offered"] * (1 - erlang_sl(c["agents_productive"], c["offered"], c["aht_seconds"]))
                            for c in scored)
        actual_miss = sum(c["offered"] - c["answered_in_sl"] for c in scored)
        if expected_miss > 0:
            miss_factor = clip(actual_miss / expected_miss, 0.2, 2.0)

    # 4. the rest of the day, interval by interval
    plans = []
    for start in sorted(s for s in forecast if s >= now):
        fc_calls, fc_aht = forecast[start]
        calls, aht = fc_calls * volume_factor, fc_aht * aht_factor
        if calls <= 0:
            continue
        agents = planned.get(start, 0.0) * productive_share
        need = agents_needed(calls, aht, sl_target, SL_SECONDS)
        plans.append(IntervalPlan(start, round(calls, 1), round(aht, 1), round(agents, 2),
                                  1 - (1 - erlang_sl(agents, calls, aht)) * miss_factor, need,
                                  round(agents - need, 2)))

    # 5. + 6. end of day
    later_calls = sum(p.calls for p in plans)
    later_in_sl = sum(p.calls * p.expected_sl for p in plans)
    total = offered + later_calls
    projected = (in_sl + later_in_sl) / total if total else None
    required = (sl_target * total - in_sl) / later_calls if later_calls else None
    if required is None:
        status = "CLOSED"
    elif required <= 0:
        status = "SECURED"                     # even 0 % from now on would close on target
    elif required > 1:
        status = "TARGET LOST"                 # even 100 % from now on would not be enough
    elif projected is not None and projected >= sl_target:
        status = "ON TRACK"
    else:
        status = "AT RISK"
    return Projection(market, now, offered, in_sl, in_sl / offered if offered else None,
                      round(volume_factor, 3), round(aht_factor, 3), round(productive_share, 3), round(miss_factor, 3),
                      round(later_calls, 1), projected, required, status, plans)


def summary_line(p: Projection) -> str:
    pct = lambda x: "  -  " if x is None else f"{100 * x:4.0f}%"        # noqa: E731
    short = [i for i in p.intervals if i.agents_gap < 0]
    worst = min(short, key=lambda i: i.agents_gap) if short else None
    gap = f"worst gap {worst.interval_start:%H:%M} {worst.agents_gap:+.1f} agents" if worst else "no interval short"
    return (f"{p.market} {p.status:<11} SL so far {pct(p.sl_so_far)} | projected EOD {pct(p.projected_sl)}"
            f" | needed from now {pct(p.required_sl)} | volume x{p.volume_factor:.2f} | {gap}")
