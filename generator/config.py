"""Settings of the simulated contact center.

Everything that makes the data "look real" lives here, so it can be tuned
without touching the simulation code.
"""
from __future__ import annotations

from dataclasses import dataclass

INTERVAL_MINUTES = 15            # reporting interval used everywhere (forecast, KPIs)
SERVICE_LEVEL_SECONDS = 20       # a contact answered within 20 s counts as "in service level"
SERVICE_LEVEL_TARGET = 0.80      # 80/20 target


@dataclass(frozen=True)
class Market:
    code: str                    # UK / DE / ES
    queue: str                   # queue name, as it would appear in the telephony export
    timezone: str                # local time zone (opening hours are local)
    language: str
    weekday_hours: tuple[int, int]   # opening hours Monday-Friday, local time
    saturday_hours: tuple[int, int] | None   # None = closed
    daily_volume: int            # forecast contacts on an average weekday
    talk_seconds: int            # average talk time
    acw_seconds: int             # average after-contact work (wrap-up)
    patience_seconds: int        # average time a caller waits before hanging up


MARKETS = [
    Market("UK", "UK_General", "Europe/London", "en", (8, 20), (9, 17), 2400, 330, 45, 150),
    Market("DE", "DE_General", "Europe/Berlin", "de", (8, 20), (9, 17), 1800, 360, 50, 170),
    Market("ES", "ES_General", "Europe/Madrid", "es", (9, 21), None, 1500, 300, 40, 130),
]

# Volume by day of week, relative to an average weekday (Monday is the busiest day)
WEEKDAY_FACTOR = {0: 1.15, 1: 1.05, 2: 1.00, 3: 0.97, 4: 0.92, 5: 0.50, 6: 0.0}

# Forecast accuracy: real volume differs from the forecast, like in real life
DAY_FORECAST_ERROR = 0.06        # standard deviation of the daily error (6 %)
INTERVAL_FORECAST_ERROR = 0.10   # standard deviation of the error per 15-minute interval

# Staffing: how many agents are scheduled compared with the workload
TARGET_OCCUPANCY = 1.0           # agents needed = workload / occupancy (1.0 = lean staffing, like a tight budget)
STAFFING_TOLERANCE = 2.5         # an interval may be up to 2.5 agents short: some intervals miss SL, like real life
SHIFT_HOURS = 8
BREAKS = [                       # (minutes after shift start, duration in minutes, state)
    (120, 15, "BREAK"),
    (240, 30, "LUNCH"),
    (360, 15, "BREAK"),
]

# Adherence behaviour: some agents do not follow their schedule exactly
LATE_LOGIN_RATE = 0.12           # 12 % of shifts start late
LATE_LOGIN_MINUTES = (3, 15)
BREAK_OVERRUN_RATE = 0.18        # 18 % of breaks last longer than planned
BREAK_OVERRUN_MINUTES = (2, 10)
EARLY_LOGOUT_RATE = 0.05
EARLY_LOGOUT_MINUTES = (5, 20)
ABSENCE_RATE = 0.06              # 6 % of scheduled agents do not show up at all
