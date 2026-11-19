-- FACT: one row per scheduled agent and day (schedule adherence and productivity).
-- Late and pause overrun use a 2-minute grace period: logging in 20 seconds after the shift start is not "late"
-- (without it, 97 % of agent-days counted as late).
select
    agent_username,
    market_code,
    to_char(work_date, 'YYYYMMDD')::int              as date_key,
    shift_start_utc,
    shift_end_utc,
    first_login_utc,
    last_logout_utc,
    is_absent,
    not is_absent and late_login_minutes >= {{ var('grace_minutes') }}       as is_late,
    not is_absent and pause_overrun_minutes >= {{ var('grace_minutes') }}    as has_pause_overrun,
    late_login_minutes,
    pause_overrun_minutes,
    contacts_handled,
    aht_seconds,
    round(productive_seconds / 3600, 2)              as productive_hours,
    adherence,
    case                                              -- colour code used on RTM wallboards
        when is_absent then 'Absent'
        when adherence >= 0.90 then 'Green'
        when adherence >= 0.85 then 'Amber'
        else 'Red'
    end                                              as adherence_band
from {{ ref('stg_agent_daily') }}
