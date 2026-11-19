-- One row per market and day (end-of-day report), clean names and types.
select
    market                                              as market_code,
    date                                                as report_date,
    forecast_contacts::numeric(9, 1)                    as forecast_contacts,
    offered::int                                        as contacts_offered,
    answered::int                                       as contacts_answered,
    abandoned::int                                      as contacts_abandoned,
    service_level::numeric(6, 4)                        as service_level,
    abandon_rate::numeric(6, 4)                         as abandon_rate,
    asa_seconds::numeric(8, 1)                          as asa_seconds,
    forecast_error::numeric(8, 4)                       as forecast_error,
    intervals_below_target::int                         as intervals_below_target,
    intervals_open::int                                 as intervals_open,
    agents_scheduled::int                               as agents_scheduled,
    agents_absent::int                                  as agents_absent,
    adherence_present_agents::numeric(6, 4)             as adherence_present_agents
from {{ source('lakehouse', 'daily_market') }}
