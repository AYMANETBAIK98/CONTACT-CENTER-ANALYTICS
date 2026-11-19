-- One row per scheduled agent and day, clean names and types.
select
    agent_username,
    market                                              as market_code,
    date                                                as work_date,
    shift_start at time zone 'UTC'                      as shift_start_utc,
    shift_end at time zone 'UTC'                        as shift_end_utc,
    first_login at time zone 'UTC'                      as first_login_utc,
    last_logout at time zone 'UTC'                      as last_logout_utc,
    is_absent,
    late_login_minutes::numeric(6, 1)                   as late_login_minutes,
    pause_overrun_minutes::numeric(6, 1)                as pause_overrun_minutes,
    contacts_handled::int                               as contacts_handled,
    aht_seconds::numeric(8, 1)                          as aht_seconds,
    productive_seconds::numeric(10, 1)                  as productive_seconds,
    adherence::numeric(6, 4)                            as adherence
from {{ source('lakehouse', 'agent_daily') }}
