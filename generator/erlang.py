"""Erlang C: the standard workforce-management formula for a call queue.

Given the calls expected in an interval and their handle time, it says how many agents
are needed to answer X % of calls within T seconds. Planners use it to build schedules,
real-time analysts use it to know how many agents an interval really needs.
"""
from __future__ import annotations

import math


def probability_of_waiting(agents: int, traffic: float) -> float:
    """Erlang C probability that a call has to wait (all agents busy).
    traffic = workload in Erlangs = calls x handle time / interval length."""
    if agents <= traffic:
        return 1.0                                    # not enough agents: the queue grows without limit
    # sum of traffic^k / k! for k < agents, computed term by term (no huge factorials)
    term, total = 1.0, 1.0
    for k in range(1, agents):
        term *= traffic / k
        total += term
    last = term * traffic / agents * agents / (agents - traffic)
    return last / (total + last)


def service_level(agents: int, calls: float, aht_seconds: float, target_seconds: float,
                  interval_seconds: float = 900) -> float:
    """Share of calls answered within target_seconds."""
    if calls <= 0:
        return 1.0
    traffic = calls * aht_seconds / interval_seconds
    if agents <= traffic:
        return 0.0
    wait = probability_of_waiting(agents, traffic)
    return 1 - wait * math.exp(-(agents - traffic) * target_seconds / aht_seconds)


def agents_needed(calls: float, aht_seconds: float, sl_target: float = 0.8, target_seconds: float = 20,
                  interval_seconds: float = 900) -> int:
    """Smallest number of agents that reaches the service level target."""
    if calls <= 0:
        return 0
    agents = max(1, math.ceil(calls * aht_seconds / interval_seconds))
    while service_level(agents, calls, aht_seconds, target_seconds, interval_seconds) < sl_target:
        agents += 1
    return agents
