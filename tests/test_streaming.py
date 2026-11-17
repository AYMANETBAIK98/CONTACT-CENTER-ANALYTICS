"""Tests for the real-time layer, without Kafka: events go straight into KpiState.

    docker compose run --rm tools pytest -v tests/test_streaming.py
"""
from datetime import datetime, timezone

import pytest

from generator.config import MARKETS
from generator.simulate import simulate_day
from streaming.events import KpiState, contact_lifecycle_events, interval_start, split_by_interval

DAY = simulate_day(MARKETS[0], datetime(2026, 11, 4).date(), seed=42)   # one UK Wednesday


def utc(h, m, s=0):
    return datetime(2026, 11, 4, h, m, s, tzinfo=timezone.utc)


def feed(state, contacts, agent_events):
    events = [(e["event_ts"], "c", e) for ctr in contacts for e in contact_lifecycle_events(ctr)]
    events += [(e["EventTimestamp"], "a", e) for e in agent_events]
    for _, kind, e in sorted(events, key=lambda x: (x[0], x[1])):
        (state.on_contact_event if kind == "c" else state.on_agent_event)(e)


def test_lifecycle_events():
    answered = next(c for c in DAY.contacts if c["Agent"])
    abandoned = next(c for c in DAY.contacts if not c["Agent"])
    assert [e["event_type"] for e in contact_lifecycle_events(answered)] == \
        ["CONTACT_QUEUED", "CONTACT_ANSWERED", "CONTACT_ENDED"]
    assert [e["event_type"] for e in contact_lifecycle_events(abandoned)] == \
        ["CONTACT_QUEUED", "CONTACT_ABANDONED"]


def test_interval_helpers():
    assert interval_start(utc(10, 7, 31)) == utc(10, 0)
    parts = list(split_by_interval(utc(10, 10), utc(10, 40)))
    assert [(b, (e - s).seconds) for b, s, e in parts] == [(utc(10, 0), 300), (utc(10, 15), 900), (utc(10, 30), 600)]


def test_streamed_kpis_equal_the_batch_truth():
    """Counting event by event must give exactly what counting the finished contacts gives."""
    state = KpiState()
    feed(state, DAY.contacts, DAY.agent_events)
    rows = state.rows()
    assert sum(r["offered"] for r in rows) == len(DAY.contacts)
    assert sum(r["answered"] for r in rows) == sum(1 for c in DAY.contacts if c["Agent"])
    in_sl = sum(1 for c in DAY.contacts if c["Agent"] and c["Queue"]["Duration"] <= 20)
    assert sum(r["answered_in_sl"] for r in rows) == in_sl


def test_a_replayed_day_is_not_counted_twice():
    once, twice = KpiState(), KpiState()
    feed(once, DAY.contacts, DAY.agent_events)
    feed(twice, DAY.contacts, DAY.agent_events)
    feed(twice, DAY.contacts, DAY.agent_events)
    assert once.rows() == twice.rows()


def test_ratios_stay_between_0_and_1():
    state = KpiState()
    feed(state, DAY.contacts, DAY.agent_events)
    for r in state.rows():
        for col in ("service_level", "abandon_rate", "occupancy"):
            assert r[col] is None or 0 <= r[col] <= 1, (col, r)


def test_adherence_of_a_perfect_agent_is_100_percent():
    plan = {"UK-A999": [(utc(9, 0), utc(9, 30), "WORK"), (utc(9, 30), utc(9, 45), "BREAK"),
                        (utc(9, 45), utc(10, 0), "WORK")]}
    state = KpiState(schedule=plan)
    for ts, st in [(utc(9, 0), "LOGIN"), (utc(9, 0), "AVAILABLE"), (utc(9, 10), "ON_CONTACT"),
                   (utc(9, 30), "BREAK"), (utc(9, 45), "AVAILABLE"), (utc(10, 0), "LOGOUT")]:
        state.on_agent_event({"EventId": f"{ts}{st}", "AgentUsername": "UK-A999", "Market": "UK",
                              "EventTimestamp": ts.isoformat().replace("+00:00", "Z"), "State": st})
    for r in state.rows():
        assert r["adherence"] == pytest.approx(1.0)


def test_adherence_drops_when_the_break_runs_long():
    plan = {"UK-A999": [(utc(9, 0), utc(9, 15), "BREAK"), (utc(9, 15), utc(9, 30), "WORK")]}
    state = KpiState(schedule=plan)
    for ts, st in [(utc(9, 0), "LOGIN"), (utc(9, 0), "BREAK"), (utc(9, 20), "AVAILABLE"), (utc(9, 30), "LOGOUT")]:
        state.on_agent_event({"EventId": f"{ts}{st}", "AgentUsername": "UK-A999", "Market": "UK",
                              "EventTimestamp": ts.isoformat().replace("+00:00", "Z"), "State": st})
    by_interval = {r["interval_start"]: r["adherence"] for r in state.rows()}
    assert by_interval[utc(9, 0)] == pytest.approx(1.0)          # on break as planned
    assert by_interval[utc(9, 15)] == pytest.approx(10 / 15)     # 5 extra minutes of break
