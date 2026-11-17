"""Simulation of one contact center day for one market.

For each market and day it produces 4 datasets, the same ones a real-time analyst works with:
  forecast      expected contacts and handle time per 15-minute interval (made the "day before")
  schedule      planned shifts, breaks and lunches of every agent
  contacts      one record per call, shaped like an Amazon Connect contact trace record (CTR)
  agent_events  every agent state change (LOGIN, AVAILABLE, ON_CONTACT, ACW, BREAK, LUNCH, LOGOUT)

How a day is simulated:
  1. Forecast: a daily volume spread over the day with a realistic double-peak curve.
  2. Schedule: shifts are added until every interval has enough agents for the forecast workload.
  3. Reality: actual volume differs from the forecast, some agents are absent, late, or overrun breaks.
  4. Queue: calls are answered in arrival order by the first agent who becomes free.
     A caller whose wait exceeds their patience hangs up (abandoned call).
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np

from generator import config as cfg
from generator.config import Market
from generator.erlang import agents_needed

INTERVAL = timedelta(minutes=cfg.INTERVAL_MINUTES)


# ---------------------------------------------------------------- helpers
def iso(ts: datetime) -> str:
    """UTC timestamp written like the telephony export: 2026-11-02T08:15:03.123Z"""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def opening_hours(market: Market, day: date) -> tuple[int, int] | None:
    if day.weekday() < 5:
        return market.weekday_hours
    if day.weekday() == 5:
        return market.saturday_hours
    return None                                   # Sunday: closed everywhere


def intraday_curve(open_h: int, close_h: int) -> list[float]:
    """Share of the daily volume in each 15-minute interval.
    Shape: calls build up after opening, peak late morning, dip at lunch,
    second (smaller) peak mid-afternoon, then fade before closing."""
    n = (close_h - open_h) * 60 // cfg.INTERVAL_MINUTES
    weights = []
    for i in range(n):
        h = open_h + (i + 0.5) * cfg.INTERVAL_MINUTES / 60          # middle of the interval, in hours
        w = (0.35
             + 1.00 * math.exp(-((h - 10.75) ** 2) / 2.2)             # morning peak ~10:45
             + 0.70 * math.exp(-((h - 15.25) ** 2) / 2.8)             # afternoon peak ~15:15
             - 0.18 * math.exp(-((h - 13.25) ** 2) / 0.5))            # lunch dip ~13:15
        weights.append(max(w, 0.05))
    total = sum(weights)
    return [w / total for w in weights]


def make_uuid(rng: np.random.Generator) -> str:
    """Reproducible UUID (same seed = same ids)."""
    return str(uuid.UUID(int=int(rng.integers(0, 2**63)) << 64 | int(rng.integers(0, 2**63)), version=4))


# ---------------------------------------------------------------- 1. forecast
@dataclass
class Interval:
    start: datetime                              # UTC
    forecast_contacts: float
    forecast_aht: int


def build_forecast(market: Market, day: date) -> list[Interval]:
    hours = opening_hours(market, day)
    if hours is None:
        return []
    tz = ZoneInfo(market.timezone)
    open_h, close_h = hours
    daily = market.daily_volume * cfg.WEEKDAY_FACTOR[day.weekday()]
    first = datetime.combine(day, time(open_h), tzinfo=tz)
    aht = market.talk_seconds + market.acw_seconds
    return [Interval((first + i * INTERVAL).astimezone(timezone.utc), round(daily * share, 1), aht)
            for i, share in enumerate(intraday_curve(open_h, close_h))]


# ---------------------------------------------------------------- 2. schedule
@dataclass
class Shift:
    agent_id: str
    start: datetime                              # UTC
    end: datetime
    breaks: list[tuple[datetime, int, str]]      # (planned start, minutes, BREAK/LUNCH)


def build_schedule(market: Market, forecast: list[Interval]) -> list[Shift]:
    """Add 8-hour shifts until each interval has (almost) the agents Erlang C says it needs."""
    if not forecast:
        return []
    n = len(forecast)
    slots_per_shift = min(cfg.SHIFT_HOURS * 60 // cfg.INTERVAL_MINUTES, n)
    # agents each interval needs to reach 80/20 with the forecast volume (Erlang C, like a WFM planner)
    needed = [agents_needed(iv.forecast_contacts, iv.forecast_aht, cfg.SERVICE_LEVEL_TARGET,
                            cfg.SERVICE_LEVEL_SECONDS, cfg.INTERVAL_MINUTES * 60) for iv in forecast]

    def productive_slots(start_slot: int) -> list[int]:
        """Intervals a shift starting at start_slot covers, minus its planned breaks."""
        off = set()
        for after_min, minutes, _ in cfg.BREAKS:
            first_off = start_slot + after_min // cfg.INTERVAL_MINUTES
            off.update(range(first_off, first_off + max(1, minutes // cfg.INTERVAL_MINUTES)))
        return [s for s in range(start_slot, start_slot + slots_per_shift) if s not in off and s < n]

    starts = range(0, n - slots_per_shift + 1, 2)            # a shift can start every 30 minutes
    # each interval may stay a little short (lean staffing), but never more than 2 agents
    # and never so short that a quiet interval (opening, closing) is left with nobody
    allowed_gap = [min(cfg.STAFFING_TOLERANCE * need, cfg.MAX_AGENTS_SHORT) for need in needed]
    coverage = [0.0] * n
    shift_starts: list[int] = []
    while True:
        deficit = [needed[i] - coverage[i] for i in range(n)]
        gain = {s: sum(max(deficit[i] - allowed_gap[i], 0) for i in productive_slots(s)) for s in starts}
        best = max(gain, key=gain.get)
        if gain[best] <= 0:                     # no shift can still reduce the gap: schedule complete
            break
        shift_starts.append(best)
        for i in productive_slots(best):
            coverage[i] += 1

    shifts = []
    for k, slot in enumerate(sorted(shift_starts), start=1):
        start = forecast[slot].start
        stagger = (k % 3 - 1) * cfg.INTERVAL_MINUTES     # -15, 0 or +15 min: not everyone on break together
        breaks = [(start + timedelta(minutes=after + stagger), minutes, state) for after, minutes, state in cfg.BREAKS
                  if after + stagger + minutes <= slots_per_shift * cfg.INTERVAL_MINUTES]
        shifts.append(Shift(f"{market.code}-A{k:03d}", start,
                            start + timedelta(minutes=slots_per_shift * cfg.INTERVAL_MINUTES), breaks))
    return shifts


# ---------------------------------------------------------------- 3 + 4. reality and queue
@dataclass
class AgentRun:
    """What one agent really does during the day."""
    shift: Shift
    login: datetime
    logout: datetime
    breaks: list[tuple[datetime, float, str]]    # (planned start, actual seconds, state)
    free_at: datetime
    next_break: int = 0
    activities: list[tuple[datetime, datetime, str]] = field(default_factory=list)

    def time_available(self, t: datetime) -> datetime | None:
        """Earliest moment >= t this agent can take a call, without changing anything.
        A break due before that moment is taken first (after the current call ends)."""
        f = max(self.free_at, self.login)
        j = self.next_break
        while j < len(self.breaks) and self.breaks[j][0] <= max(t, f):
            planned, seconds, _ = self.breaks[j]
            f = max(planned, f) + timedelta(seconds=seconds)
            j += 1
        ready = max(t, f)
        return ready if ready < self.logout else None

    def take_breaks_until(self, t: datetime) -> None:
        """Record the breaks that start before t (their timing is now certain)."""
        while self.next_break < len(self.breaks):
            planned, seconds, state = self.breaks[self.next_break]
            start = max(planned, self.free_at, self.login)
            if planned > t or start >= self.logout:
                break
            end = start + timedelta(seconds=seconds)
            self.activities.append((start, end, state))
            self.free_at = end
            self.next_break += 1


def agent_reality(shifts: list[Shift], rng: np.random.Generator) -> list[AgentRun]:
    runs = []
    for s in shifts:
        if rng.random() < cfg.ABSENCE_RATE:
            continue                                                  # absent: no login at all
        login = s.start + timedelta(seconds=float(rng.uniform(0, 90)))
        if rng.random() < cfg.LATE_LOGIN_RATE:
            login += timedelta(minutes=float(rng.uniform(*cfg.LATE_LOGIN_MINUTES)))
        logout = s.end - timedelta(seconds=float(rng.uniform(0, 60)))
        if rng.random() < cfg.EARLY_LOGOUT_RATE:
            logout -= timedelta(minutes=float(rng.uniform(*cfg.EARLY_LOGOUT_MINUTES)))
        breaks = []
        for planned, minutes, state in s.breaks:
            seconds = minutes * 60 + float(rng.uniform(-30, 45))
            if rng.random() < cfg.BREAK_OVERRUN_RATE:
                seconds += float(rng.uniform(*cfg.BREAK_OVERRUN_MINUTES)) * 60
            breaks.append((planned, seconds, state))
        runs.append(AgentRun(s, login, logout, breaks, free_at=login))
    return runs


def simulate_contacts(market: Market, forecast: list[Interval], agents: list[AgentRun],
                      rng: np.random.Generator) -> list[dict]:
    if not forecast:
        return []
    # Actual volume = forecast x daily error x interval error, then Poisson randomness
    day_factor = max(0.5, rng.normal(1, cfg.DAY_FORECAST_ERROR))
    arrivals = []
    for iv in forecast:
        expected = iv.forecast_contacts * day_factor * max(0.3, rng.normal(1, cfg.INTERVAL_FORECAST_ERROR))
        for _ in range(rng.poisson(expected)):
            arrivals.append(iv.start + timedelta(seconds=float(rng.uniform(0, INTERVAL.total_seconds()))))
    arrivals.sort()

    # lognormal handle times: most calls near the average, a few very long ones
    def lognormal(mean: float, sigma: float) -> float:
        return float(rng.lognormal(math.log(mean) - sigma ** 2 / 2, sigma))

    contacts = []
    for t in arrivals:
        for a in agents:
            a.take_breaks_until(t)
        patience = float(rng.exponential(market.patience_seconds))
        options = [(a.time_available(t), a) for a in agents]
        options = [(ready, a) for ready, a in options if ready is not None]
        record = {
            "ContactId": make_uuid(rng),
            "Channel": "VOICE",
            "InitiationMethod": "INBOUND",
            "InitiationTimestamp": iso(t),
            "Queue": {"Name": market.queue, "EnqueueTimestamp": iso(t)},
            "Attributes": {"market": market.code, "language": market.language},
        }
        ready, agent = min(options, key=lambda o: o[0]) if options else (None, None)
        if ready is None or (ready - t).total_seconds() > patience:
            # nobody free before the caller lost patience: abandoned in queue
            gone = t + timedelta(seconds=patience)
            record["Queue"].update(DequeueTimestamp=iso(gone), Duration=round(patience))
            record.update(Agent=None, DisconnectTimestamp=iso(gone), DisconnectReason="CUSTOMER_DISCONNECT")
        else:
            agent.take_breaks_until(ready)
            talk, acw = lognormal(market.talk_seconds, 0.6), lognormal(market.acw_seconds, 0.4)
            talk_end = ready + timedelta(seconds=talk)
            acw_end = talk_end + timedelta(seconds=acw)
            agent.activities += [(ready, talk_end, "ON_CONTACT"), (talk_end, acw_end, "ACW")]
            agent.free_at = acw_end
            record["Queue"].update(DequeueTimestamp=iso(ready), Duration=round((ready - t).total_seconds()))
            record.update(
                Agent={
                    "Username": agent.shift.agent_id,
                    "ConnectedToAgentTimestamp": iso(ready),
                    "AgentInteractionDuration": round(talk),
                    "AfterContactWorkStartTimestamp": iso(talk_end),
                    "AfterContactWorkEndTimestamp": iso(acw_end),
                    "AfterContactWorkDuration": round(acw),
                },
                DisconnectTimestamp=iso(talk_end),
                DisconnectReason="CUSTOMER_DISCONNECT" if rng.random() < 0.8 else "AGENT_DISCONNECT",
            )
        contacts.append(record)

    for a in agents:                                   # breaks still due after the last call
        a.take_breaks_until(a.logout)
    return contacts


def agent_events(market: Market, agents: list[AgentRun], rng: np.random.Generator) -> list[dict]:
    """Turn each agent's activities into state-change events, filling the gaps with AVAILABLE."""
    events = []

    def emit(agent_id: str, ts: datetime, state: str) -> None:
        events.append({"EventId": make_uuid(rng), "AgentUsername": agent_id, "Market": market.code,
                       "EventTimestamp": iso(ts), "State": state})

    for a in agents:
        agent_id = a.shift.agent_id
        emit(agent_id, a.login, "LOGIN")
        cursor = a.login
        for start, end, activity in sorted(a.activities):
            if start > cursor:                         # idle gap = waiting for a call
                emit(agent_id, cursor, "AVAILABLE")
            emit(agent_id, start, activity)
            cursor = end
        end_of_day = max(a.logout, cursor)
        if end_of_day > cursor:
            emit(agent_id, cursor, "AVAILABLE")
        emit(agent_id, end_of_day, "LOGOUT")
    events.sort(key=lambda e: e["EventTimestamp"])
    return events


# ---------------------------------------------------------------- one day
@dataclass
class DayData:
    forecast: list[Interval]
    schedule: list[Shift]
    contacts: list[dict]
    agent_events: list[dict]
    present_agents: int


def simulate_day(market: Market, day: date, seed: int) -> DayData:
    # one independent random stream per market and day: same seed = same data
    rng = np.random.default_rng([seed, day.toordinal(), ord(market.code[0]), ord(market.code[1])])
    forecast = build_forecast(market, day)
    schedule = build_schedule(market, forecast)
    agents = agent_reality(schedule, rng)
    contacts = simulate_contacts(market, forecast, agents, rng)
    events = agent_events(market, agents, rng)
    return DayData(forecast, schedule, contacts, events, len(agents))
