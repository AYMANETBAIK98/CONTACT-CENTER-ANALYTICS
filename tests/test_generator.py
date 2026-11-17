"""Tests for the data generator: each test checks one rule real contact center data always follows.

    docker compose run --rm tools pytest -v tests/test_generator.py
"""
from collections import defaultdict
from datetime import date, datetime

import pytest

from generator.config import MARKETS
from generator.simulate import simulate_day

DAY = date(2026, 11, 4)                     # a Wednesday
UK = MARKETS[0]


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@pytest.fixture(scope="module")
def day():
    return simulate_day(UK, DAY, seed=42)


def test_same_seed_gives_same_data(day):
    again = simulate_day(UK, DAY, seed=42)
    assert [c["ContactId"] for c in again.contacts] == [c["ContactId"] for c in day.contacts]


def test_other_seed_gives_other_data(day):
    other = simulate_day(UK, DAY, seed=7)
    assert other.contacts[0]["ContactId"] != day.contacts[0]["ContactId"]


def test_sunday_is_closed():
    sunday = simulate_day(UK, date(2026, 11, 8), seed=42)
    assert sunday.forecast == [] and sunday.contacts == [] and sunday.schedule == []


def test_volume_close_to_forecast(day):
    forecast = sum(iv.forecast_contacts for iv in day.forecast)
    assert 0.75 * forecast <= len(day.contacts) <= 1.25 * forecast


def test_timestamps_follow_the_call_lifecycle(day):
    """queued -> answered -> talk ends -> wrap-up ends, never backwards."""
    for c in day.contacts:
        start, dequeue = ts(c["Queue"]["EnqueueTimestamp"]), ts(c["Queue"]["DequeueTimestamp"])
        assert dequeue >= start
        if c["Agent"]:
            a = c["Agent"]
            assert ts(a["ConnectedToAgentTimestamp"]) == dequeue
            assert ts(a["AfterContactWorkStartTimestamp"]) >= dequeue
            assert ts(a["AfterContactWorkEndTimestamp"]) >= ts(a["AfterContactWorkStartTimestamp"])


def test_an_agent_never_takes_two_calls_at_once(day):
    calls = defaultdict(list)
    for c in day.contacts:
        if c["Agent"]:
            calls[c["Agent"]["Username"]].append(
                (ts(c["Agent"]["ConnectedToAgentTimestamp"]), ts(c["Agent"]["AfterContactWorkEndTimestamp"])))
    for agent, spans in calls.items():
        spans.sort()
        for (_, end), (next_start, _) in zip(spans, spans[1:]):
            assert next_start >= end, f"{agent} overlaps"


def test_no_call_answered_during_a_break(day):
    """Rebuild each agent's timeline from the state events: a call never starts while on BREAK or LUNCH."""
    events = defaultdict(list)
    for e in day.agent_events:
        events[e["AgentUsername"]].append((ts(e["EventTimestamp"]), e["State"]))
    pauses = defaultdict(list)                  # (start, end) of every BREAK / LUNCH, from consecutive events
    for agent, evs in events.items():
        for (start, state), (end, _) in zip(evs, evs[1:]):
            if state in ("BREAK", "LUNCH"):
                pauses[agent].append((start, end))
    for c in day.contacts:
        if c["Agent"]:
            connected = ts(c["Agent"]["ConnectedToAgentTimestamp"])
            for start, end in pauses[c["Agent"]["Username"]]:
                assert not (start <= connected < end), c["ContactId"]


def test_agent_day_starts_with_login_and_ends_with_logout(day):
    states = defaultdict(list)
    for e in day.agent_events:
        states[e["AgentUsername"]].append(e["State"])
    assert states
    for agent, s in states.items():
        assert s[0] == "LOGIN" and s[-1] == "LOGOUT", agent


def test_kpis_in_a_realistic_range(day):
    """Not perfect, not broken: the kind of day a real-time analyst sees."""
    answered = [c for c in day.contacts if c["Agent"]]
    abandon = 1 - len(answered) / len(day.contacts)
    service_level = sum(c["Queue"]["Duration"] <= 20 for c in answered) / len(day.contacts)
    assert 0.0 < abandon < 0.20
    assert 0.60 < service_level < 0.99


def test_absent_agents_have_no_events(day):
    scheduled = {s.agent_id for s in day.schedule}
    present = {e["AgentUsername"] for e in day.agent_events}
    assert present <= scheduled
    assert len(present) == day.present_agents
