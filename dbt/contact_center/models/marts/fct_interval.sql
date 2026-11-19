-- FACT: one row per market and 15-minute interval.
-- Keys to the dimensions (market_code, date_key, interval_key) + the measures.
-- Counts are stored (offered, answered...) so Power BI can re-aggregate correctly:
-- the service level of a day = sum(answered_in_sl) / sum(offered), NOT the average of interval SLs.
with kpis as (
    select k.*, m.timezone, m.sl_target
    from {{ ref('stg_interval_kpis') }} k
    join {{ ref('dim_market') }} m using (market_code)
),

local_time as (
    select
        *,
        (interval_start_utc at time zone timezone) as interval_start_local
    from kpis
)

select
    market_code,
    to_char(interval_date, 'YYYYMMDD')::int                                          as date_key,
    (extract(hour from interval_start_local) * 4
        + extract(minute from interval_start_local) / 15)::int                       as interval_key,
    interval_start_utc,
    interval_start_local,

    -- counts (additive: can be summed over any period)
    forecast_contacts,
    contacts_offered,
    contacts_answered,
    contacts_abandoned,
    contacts_answered_in_sl,
    contacts_offered - forecast_contacts                                             as forecast_gap_contacts,
    agents_scheduled,
    agents_productive,
    agents_productive - agents_scheduled                                             as staffing_gap_agents,

    -- ratios of the interval (not additive: re-compute them from the counts for longer periods)
    service_level,
    abandon_rate,
    asa_seconds,
    aht_seconds,
    occupancy,
    adherence,

    -- flags a real-time analyst filters on
    contacts_offered > 0                                                             as has_contacts,
    contacts_offered > 0 and service_level < sl_target                               as is_below_sl_target
from local_time
