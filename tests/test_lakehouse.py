"""Tests for the Spark lakehouse jobs, on one simulated day written to a temporary local "lake".

    docker compose run --rm spark pytest -v tests/test_lakehouse.py

(Spark is only in the "spark" container: in the tools container these tests are skipped.)
"""
import json
from datetime import date

import pytest

pyspark = pytest.importorskip("pyspark")

from generator.config import MARKETS                       # noqa: E402
from generator.generate import write_day                    # noqa: E402
from generator.simulate import simulate_day                 # noqa: E402
from streaming.events import KpiState, contact_lifecycle_events  # noqa: E402

DAY = date(2026, 11, 4)
UK = MARKETS[0]


@pytest.fixture(scope="module")
def lake(tmp_path_factory, monkeypatch_module):
    """Write one UK day as bronze (+ 1 duplicated and 1 broken contact), run silver and gold."""
    root = tmp_path_factory.mktemp("lake")
    import generator.generate as gen
    monkeypatch_module.setattr(gen, "LANDING", root / "bronze")
    data = simulate_day(UK, DAY, seed=42)
    write_day(DAY, UK.code, data)
    contacts_file = root / "bronze" / "contacts" / f"date={DAY}" / "market=UK" / "contacts.jsonl"
    lines = contacts_file.read_text().splitlines()
    broken = json.loads(lines[1])
    broken["ContactId"] = "broken-1"
    broken["Queue"]["Duration"] = -5                          # impossible value -> quarantine
    contacts_file.write_text("\n".join(lines + [lines[0], json.dumps(broken)]) + "\n")   # + duplicate + broken

    import spark.lake as lake_mod
    monkeypatch_module.setattr(lake_mod, "LAKE_URI", f"file://{root}/")
    import spark.gold as gold
    import spark.silver as silver
    monkeypatch_module.setattr(silver, "path", lake_mod.path)
    monkeypatch_module.setattr(gold, "path", lake_mod.path)
    s = lake_mod.spark_session("tests")
    contacts, bad_contacts = silver.build_contacts(s)
    states, bad_events = silver.build_agent_states(s)
    silver.write(contacts, "contacts")
    silver.write(states, "agent_states")
    silver.write(silver.build_schedule(s), "schedule")
    silver.write(silver.build_forecast(s), "forecast")
    silver.write(bad_contacts.unionByName(bad_events), "quarantine")
    c, st = gold.read(s, "contacts"), gold.read(s, "agent_states")
    sc, fc = gold.read(s, "schedule"), gold.read(s, "forecast")
    kpis = gold.build_interval_kpis(c, st, sc, fc).toPandas()
    daily = gold.build_agent_daily(c, st, sc).toPandas()
    quarantine = s.read.parquet(lake_mod.path("silver", "quarantine")).toPandas()
    n_contacts = c.count()
    yield {"data": data, "kpis": kpis, "daily": daily, "quarantine": quarantine, "contacts": n_contacts}
    s.stop()


@pytest.fixture(scope="module")
def monkeypatch_module():
    mp = pytest.MonkeyPatch()
    yield mp
    mp.undo()


def test_duplicates_removed_and_broken_rows_quarantined(lake):
    assert lake["contacts"] == len(lake["data"].contacts)              # duplicate kept once, broken row out
    assert list(lake["quarantine"]["reason"]) == ["negative wait"]


def test_batch_kpis_equal_streaming_kpis(lake):
    """The lakehouse (batch) and the Kafka consumer (streaming) must tell the same story."""
    state = KpiState()
    events = [(e["event_ts"], "c", e) for ctr in lake["data"].contacts for e in contact_lifecycle_events(ctr)]
    events += [(e["EventTimestamp"], "a", e) for e in lake["data"].agent_events]
    for _, kind, e in sorted(events, key=lambda x: (x[0], x[1])):
        (state.on_contact_event if kind == "c" else state.on_agent_event)(e)
    stream = {(r["market"], r["interval_start"].replace(tzinfo=None)): r for r in state.rows()}
    batch = lake["kpis"]
    assert batch["offered"].sum() == sum(r["offered"] for r in stream.values())
    for _, b in batch[batch["offered"] > 0].iterrows():
        s = stream[(b["market"], b["interval_start"].to_pydatetime().replace(tzinfo=None))]
        assert b["answered_in_sl"] == s["answered_in_sl"]
        assert b["occupancy"] == pytest.approx(s["occupancy"], abs=1e-6)


def test_absent_agents_are_flagged(lake):
    daily = lake["daily"]
    assert daily["is_absent"].sum() == len(lake["data"].schedule) - lake["data"].present_agents


def test_adherence_between_0_and_1(lake):
    present = lake["daily"][~lake["daily"]["is_absent"]]
    assert present["adherence"].between(0, 1).all()
