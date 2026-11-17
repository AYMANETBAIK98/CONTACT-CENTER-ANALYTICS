"""Erlang C checked against published table values."""
import pytest

from generator.erlang import agents_needed, probability_of_waiting, service_level


def test_probability_of_waiting_matches_erlang_tables():
    # 10 Erlangs of traffic: 11 agents -> 68.2 % of calls wait, 14 agents -> 17.4 %
    assert probability_of_waiting(11, 10) == pytest.approx(0.6821, abs=1e-4)
    assert probability_of_waiting(14, 10) == pytest.approx(0.1741, abs=1e-4)


def test_too_few_agents_means_everybody_waits():
    assert probability_of_waiting(10, 10) == 1.0
    assert service_level(10, calls=100, aht_seconds=180, target_seconds=20, interval_seconds=1800) == 0.0


def test_agents_needed_is_the_smallest_number_reaching_the_target():
    n = agents_needed(100, 180, sl_target=0.8, target_seconds=20, interval_seconds=1800)
    assert n == 14
    assert service_level(n, 100, 180, 20, 1800) >= 0.8 > service_level(n - 1, 100, 180, 20, 1800)


def test_no_calls_no_agents():
    assert agents_needed(0, 300) == 0
