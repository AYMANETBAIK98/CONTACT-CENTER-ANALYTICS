"""Tests for the real-time analyst logic (step 5): end-of-day SL projection.

    docker compose run --rm tools pytest -v tests/test_rta.py
"""
from datetime import date, datetime, timedelta, timezone

import pytest

from rta.projection import project

T0 = datetime(2026, 11, 4, 8, 0, tzinfo=timezone.utc)
Q = timedelta(minutes=15)


def day(n_intervals=16, calls=40.0, aht=375.0, agents=22.0):
    starts = [T0 + i * Q for i in range(n_intervals)]
    return starts, {s: (calls, aht) for s in starts}, {s: agents for s in starts}


def closed_rows(starts, offered, in_sl, productive=22.0, aht=375.0):
    return [{"interval_start": s, "offered": offered, "answered": offered, "answered_in_sl": in_sl,
             "aht_seconds": aht, "agents_productive": productive} for s in starts]


def test_required_sl_arithmetic():
    """8 intervals x 40 calls done at 70 %, 8 x 40 left: closing at 80 % needs 90 % from now on."""
    starts, fc, plan = day()
    p = project("UK", starts[8], closed_rows(starts[:8], 40, 28), fc, plan)
    assert p.offered_so_far == 320 and p.in_sl_so_far == 224
    assert p.calls_remaining == pytest.approx(320)
    assert p.required_sl == pytest.approx((0.8 * 640 - 224) / 320)     # 0.90


def test_volume_reforecast():
    """Today runs 25 % above forecast: the rest of the day is re-forecast 25 % higher."""
    starts, fc, plan = day()
    p = project("UK", starts[8], closed_rows(starts[:8], 50, 45), fc, plan)
    assert p.volume_factor == pytest.approx(1.25)
    assert p.calls_remaining == pytest.approx(8 * 40 * 1.25)


def test_status_target_lost_and_secured():
    starts, fc, plan = day()
    lost = project("UK", starts[14], closed_rows(starts[:14], 40, 10), fc, plan)
    assert lost.status == "TARGET LOST" and lost.required_sl > 1
    secured = project("UK", starts[14], closed_rows(starts[:14], 40, 40), fc, plan)
    assert secured.status == "SECURED" and secured.required_sl <= 0


def test_short_staffed_intervals_show_a_gap():
    """Half the planned agents in the afternoon: those intervals are flagged short, the projection drops."""
    starts, fc, plan = day()
    full = project("UK", starts[8], closed_rows(starts[:8], 40, 36), fc, plan)
    for s in starts[8:]:
        plan[s] = 6.0
    short = project("UK", starts[8], closed_rows(starts[:8], 40, 36), fc, plan)
    assert all(i.agents_gap < 0 for i in short.intervals)
    assert short.projected_sl < full.projected_sl


def test_projection_on_a_simulated_day(tmp_path, monkeypatch):
    """On a full simulated day, the noon projection lands within 5 SL points of the real close."""
    import generator.generate as gen
    from generator.config import MARKETS
    from generator.simulate import simulate_day
    from rta.data import load_forecast, load_planned_agents, replay_kpis
    monkeypatch.setattr(gen, "LANDING", tmp_path)
    d = date(2026, 11, 4)
    gen.write_day(d, "UK", simulate_day(MARKETS[0], d, seed=42))
    rows = replay_kpis(str(d), tmp_path)["UK"]
    actual = sum(r["answered_in_sl"] for r in rows) / sum(r["offered"] for r in rows)
    noon = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
    p = project("UK", noon, rows, load_forecast(str(d), tmp_path)["UK"], load_planned_agents(str(d), tmp_path)["UK"])
    assert abs(p.projected_sl - actual) < 0.05


# ---------------------------------------------------------------- 5b. live adherence
def ev(agent, hhmm, state):
    return {"AgentUsername": agent, "EventTimestamp": f"2026-11-04T{hhmm}:00.000Z", "State": state}


def ts(hhmm):
    return datetime.fromisoformat(f"2026-11-04T{hhmm}:00+00:00")


PLAN = [(ts("08:00"), ts("10:00"), "WORK"), (ts("10:00"), ts("10:15"), "BREAK"), (ts("10:15"), ts("12:00"), "WORK")]


def test_pause_overrun_is_flagged_with_its_duration():
    from rta.adherence import agent_status
    events = [ev("UK-A1", "08:00", "LOGIN"), ev("UK-A1", "08:00", "AVAILABLE"), ev("UK-A1", "10:00", "BREAK")]
    s = agent_status("UK-A1", PLAN, events, ts("10:25"))          # break planned until 10:15, still on it
    assert s.exception == "PAUSE OVERRUN" and s.minutes_out == 10
    assert s.adherence == pytest.approx((145 - 10) / 145)          # 10 of 145 scheduled minutes out


def test_absent_and_perfect_agents():
    from rta.adherence import agent_status
    absent = agent_status("UK-A2", PLAN, [], ts("09:00"))
    assert absent.exception == "NOT LOGGED IN" and absent.band == "Absent" and absent.minutes_out == 60
    events = [ev("UK-A3", "08:00", "LOGIN"), ev("UK-A3", "08:00", "AVAILABLE"), ev("UK-A3", "10:00", "BREAK"),
              ev("UK-A3", "10:15", "AVAILABLE")]
    perfect = agent_status("UK-A3", PLAN, events, ts("11:00"))
    assert perfect.exception is None and perfect.adherence == 1 and perfect.band == "Green"
    assert agent_status("UK-A3", PLAN, events, ts("07:30")).band == "Not started"


def test_wallboard_adherence_equals_streaming_kpis(tmp_path, monkeypatch):
    """Agent by agent (wallboard) or interval by interval (Kafka consumer, step 2): same adherence for the market."""
    import generator.generate as gen
    from generator.config import MARKETS
    from generator.simulate import simulate_day
    from rta.wallboard import statuses
    from streaming.events import KpiState, load_schedule
    from streaming.producer import CONTACT_TOPIC, day_events
    monkeypatch.setattr(gen, "LANDING", tmp_path)
    d = date(2026, 11, 4)
    gen.write_day(d, "UK", simulate_day(MARKETS[0], d, seed=42))
    end = datetime(2026, 11, 4, 23, 0, tzinfo=timezone.utc)
    agents = [s for s in statuses(str(d), end, tmp_path) if s.scheduled_minutes]
    wallboard = sum(s.adherence * s.scheduled_minutes for s in agents) / sum(s.scheduled_minutes for s in agents)

    state = KpiState()
    state.add_schedule(load_schedule(tmp_path, str(d)))
    for _, topic, e in day_events(str(d), tmp_path):
        (state.on_contact_event if topic == CONTACT_TOPIC else state.on_agent_event)(e)
    state.advance()
    streaming = (sum(k.adherent_seconds for k in state.kpis.values())
                 / sum(k.scheduled_seconds for k in state.kpis.values()))
    assert wallboard == pytest.approx(streaming, abs=1e-6)
