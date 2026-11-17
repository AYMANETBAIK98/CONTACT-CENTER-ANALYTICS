"""Shared logic of the real-time layer (no Kafka, no database here, so it can be unit-tested).

1. contact_lifecycle_events(): turns one contact record (CTR) into the events a telephony
   platform emits live: QUEUED -> ANSWERED or ABANDONED -> ENDED.
2. KpiState: receives contact and agent events one by one, in any order, and keeps the
   KPIs of every (market, 15-minute interval) up to date.
"""
from __future__ import annotations

import csv
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

INTERVAL = timedelta(minutes=15)
SL_SECONDS = 20
PRODUCTIVE = {"AVAILABLE", "ON_CONTACT", "ACW"}     # logged in and ready or working on a contact
BUSY = {"ON_CONTACT", "ACW"}
PAUSES = {"BREAK", "LUNCH"}


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def interval_start(ts: datetime) -> datetime:
    """10:07:31 -> 10:00:00 (start of its 15-minute interval)."""
    return ts.replace(minute=ts.minute - ts.minute % 15, second=0, microsecond=0)


# ---------------------------------------------------------------- 1. contact events
def contact_lifecycle_events(ctr: dict) -> list[dict]:
    q, agent = ctr["Queue"], ctr["Agent"]
    base = {"contact_id": ctr["ContactId"], "market": ctr["Attributes"]["market"], "queue": q["Name"],
            "enqueue_ts": q["EnqueueTimestamp"]}
    events = [{**base, "event_type": "CONTACT_QUEUED", "event_ts": q["EnqueueTimestamp"]}]
    if agent is None:
        events.append({**base, "event_type": "CONTACT_ABANDONED", "event_ts": q["DequeueTimestamp"],
                       "wait_seconds": q["Duration"]})
    else:
        events.append({**base, "event_type": "CONTACT_ANSWERED", "event_ts": agent["ConnectedToAgentTimestamp"],
                       "wait_seconds": q["Duration"], "agent": agent["Username"]})
        events.append({**base, "event_type": "CONTACT_ENDED", "event_ts": agent["AfterContactWorkEndTimestamp"],
                       "agent": agent["Username"], "talk_seconds": agent["AgentInteractionDuration"],
                       "acw_seconds": agent["AfterContactWorkDuration"]})
    return events


# ---------------------------------------------------------------- schedule (for adherence)
def load_schedule(landing: Path, day: str) -> dict[str, list[tuple[datetime, datetime, str]]]:
    """agent -> planned activities. A shift is cut into WORK pieces around its breaks and lunch."""
    plans: dict[str, list[tuple[datetime, datetime, str]]] = {}
    for path in sorted((landing / "schedule" / f"date={day}").glob("market=*/schedule.csv")):
        rows = defaultdict(list)
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows[r["agent_username"]].append((parse_ts(r["start_utc"]), parse_ts(r["end_utc"]), r["activity"]))
        for agent, acts in rows.items():
            shift = next(a for a in acts if a[2] == "SHIFT")
            pauses = sorted(a for a in acts if a[2] != "SHIFT")
            pieces, cursor = [], shift[0]
            for start, end, kind in pauses:
                if start > cursor:
                    pieces.append((cursor, start, "WORK"))
                pieces.append((start, end, kind))
                cursor = end
            if shift[1] > cursor:
                pieces.append((cursor, shift[1], "WORK"))
            plans[agent] = pieces
    return plans


def split_by_interval(start: datetime, end: datetime):
    """[10:10, 10:40) -> (10:00, 300 s), (10:15, 900 s), (10:30, 600 s)"""
    cursor = start
    while cursor < end:
        bucket = interval_start(cursor)
        stop = min(end, bucket + INTERVAL)
        yield bucket, cursor, stop
        cursor = stop


def overlap(a0: datetime, a1: datetime, b0: datetime, b1: datetime) -> float:
    return max(0.0, (min(a1, b1) - max(a0, b0)).total_seconds())


# ---------------------------------------------------------------- 2. KPI state
@dataclass
class IntervalKpis:
    offered: int = 0
    answered: int = 0
    answered_in_sl: int = 0
    abandoned: int = 0
    wait_answered: float = 0.0        # sum of waits of answered contacts (for ASA)
    handled: int = 0                  # contacts ended (talk + wrap-up known)
    handle_seconds: float = 0.0       # sum of talk + wrap-up (for AHT)
    productive_seconds: float = 0.0   # agent time AVAILABLE + ON_CONTACT + ACW
    busy_seconds: float = 0.0         # agent time ON_CONTACT + ACW
    scheduled_seconds: float = 0.0    # planned agent time (work + breaks) from the schedule
    adherent_seconds: float = 0.0     # time where the agent did what the schedule planned


@dataclass
class KpiState:
    """All KPIs, kept up to date as events arrive. Contacts are counted in the interval they were
    queued in (like telephony reports), agent time is cut at interval boundaries."""
    schedule: dict[str, list[tuple[datetime, datetime, str]]] = field(default_factory=dict)
    kpis: dict[tuple[str, datetime], IntervalKpis] = field(default_factory=lambda: defaultdict(IntervalKpis))
    agent_state: dict[str, tuple[str, datetime, str]] = field(default_factory=dict)   # agent -> (state, since, market)
    dirty: set[tuple[str, datetime]] = field(default_factory=set)                    # changed since last flush
    clock: datetime = datetime.min.replace(tzinfo=timezone.utc)                      # latest event time seen

    seen: set[str] = field(default_factory=set)                                       # ids already counted

    def __post_init__(self) -> None:
        plans, self.schedule = self.schedule, {}
        self.add_schedule(plans)

    def add_schedule(self, plans: dict[str, list[tuple[datetime, datetime, str]]]) -> None:
        """Planned time is known in advance (one day at a time): count it once, per interval."""
        for agent, pieces in plans.items():
            self.schedule.setdefault(agent, []).extend(pieces)
            for start, end, _ in pieces:
                for bucket, s, e in split_by_interval(start, end):
                    self._bump(agent.split("-")[0], bucket).scheduled_seconds += (e - s).total_seconds()

    def first_time(self, event_id: str) -> bool:
        """Deduplication: a replayed event (same id) is ignored, so KPIs never count it twice."""
        if event_id in self.seen:
            return False
        self.seen.add(event_id)
        return True

    def _bump(self, market: str, bucket: datetime) -> IntervalKpis:
        self.dirty.add((market, bucket))
        return self.kpis[(market, bucket)]

    # ------------------------------------------------ contacts
    def on_contact_event(self, e: dict) -> None:
        if not self.first_time(f'{e["contact_id"]}/{e["event_type"]}'):
            return
        ts = parse_ts(e["event_ts"])
        self.clock = max(self.clock, ts)
        k = self._bump(e["market"], interval_start(parse_ts(e["enqueue_ts"])))
        kind = e["event_type"]
        if kind == "CONTACT_QUEUED":
            k.offered += 1
        elif kind == "CONTACT_ANSWERED":
            k.answered += 1
            k.wait_answered += e["wait_seconds"]
            k.answered_in_sl += e["wait_seconds"] <= SL_SECONDS
        elif kind == "CONTACT_ABANDONED":
            k.abandoned += 1
        elif kind == "CONTACT_ENDED":
            k.handled += 1
            k.handle_seconds += e["talk_seconds"] + e["acw_seconds"]

    # ------------------------------------------------ agents
    def on_agent_event(self, e: dict) -> None:
        if not self.first_time(e["EventId"]):
            return
        agent, ts, state = e["AgentUsername"], parse_ts(e["EventTimestamp"]), e["State"]
        self.clock = max(self.clock, ts)
        previous = self.agent_state.get(agent)
        if previous:
            self._account(agent, previous[0], previous[1], ts, previous[2])
        if state == "LOGOUT":
            self.agent_state.pop(agent, None)
        else:
            self.agent_state[agent] = (state, ts, e["Market"])

    def _account(self, agent: str, state: str, start: datetime, end: datetime, market: str) -> None:
        """Add the time spent in `state` between start and end to each interval it covers."""
        plan = self.schedule.get(agent, [])
        for bucket, s, e in split_by_interval(start, end):
            k = self._bump(market, bucket)
            seconds = (e - s).total_seconds()
            if state in PRODUCTIVE:
                k.productive_seconds += seconds
            if state in BUSY:
                k.busy_seconds += seconds
            for p0, p1, planned in plan:          # adherent = doing what the schedule says
                if (planned == "WORK" and state in PRODUCTIVE) or (planned in PAUSES and state in PAUSES):
                    k.adherent_seconds += overlap(s, e, p0, p1)

    # ------------------------------------------------ output
    def rows(self, keys=None) -> list[dict]:
        out = []
        for market, bucket in sorted(keys if keys is not None else self.kpis):
            k = self.kpis[(market, bucket)]
            out.append({
                "market": market,
                "interval_start": bucket,
                "offered": k.offered,
                "answered": k.answered,
                "abandoned": k.abandoned,
                "answered_in_sl": k.answered_in_sl,
                "service_level": k.answered_in_sl / k.offered if k.offered else None,
                "abandon_rate": k.abandoned / k.offered if k.offered else None,
                "asa_seconds": k.wait_answered / k.answered if k.answered else None,
                "aht_seconds": k.handle_seconds / k.handled if k.handled else None,
                "occupancy": k.busy_seconds / k.productive_seconds if k.productive_seconds else None,
                "adherence": k.adherent_seconds / k.scheduled_seconds if k.scheduled_seconds else None,
                "agents_productive": k.productive_seconds / INTERVAL.total_seconds(),
            })
        return out

    def advance(self) -> None:
        """Count the time agents have spent in their CURRENT state up to the latest event time.
        Without this, an agent on a 30-minute lunch would only be counted when the lunch ends."""
        for agent, (state, since, market) in list(self.agent_state.items()):
            if since < self.clock:
                self._account(agent, state, since, self.clock, market)
                self.agent_state[agent] = (state, self.clock, market)

    def take_dirty(self) -> list[dict]:
        self.advance()
        keys, self.dirty = self.dirty, set()
        return self.rows(keys)
