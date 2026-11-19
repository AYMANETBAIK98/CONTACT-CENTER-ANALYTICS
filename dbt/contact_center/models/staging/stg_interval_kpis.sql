-- One row per market and 15-minute interval.
-- Staging = same rows as the source, with clean names and types:
--   * timestamps written by Spark are UTC -> timestamptz
--   * ratios and seconds: double precision -> numeric (exact, and round() works on it)
select
    market                                              as market_code,
    date                                                as interval_date,
    interval_start at time zone 'UTC'                   as interval_start_utc,
    forecast_contacts::numeric(8, 1)                    as forecast_contacts,
    forecast_aht_seconds::int                           as forecast_aht_seconds,
    offered::int                                        as contacts_offered,
    answered::int                                       as contacts_answered,
    abandoned::int                                      as contacts_abandoned,
    answered_in_sl::int                                 as contacts_answered_in_sl,
    service_level::numeric(6, 4)                        as service_level,
    abandon_rate::numeric(6, 4)                         as abandon_rate,
    asa_seconds::numeric(8, 1)                          as asa_seconds,
    aht_seconds::numeric(8, 1)                          as aht_seconds,
    forecast_error::numeric(8, 4)                       as forecast_error,
    occupancy::numeric(6, 4)                            as occupancy,
    adherence::numeric(6, 4)                            as adherence,
    agents_productive::numeric(7, 2)                    as agents_productive,
    agents_scheduled::numeric(7, 2)                     as agents_scheduled
from {{ source('lakehouse', 'interval_kpis') }}
