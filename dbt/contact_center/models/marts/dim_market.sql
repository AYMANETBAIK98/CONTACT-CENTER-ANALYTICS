-- One row per market. Comes from the seed seeds/markets.csv (reference data kept in Git).
select
    market_code,
    market_name,
    queue,
    timezone,
    language,
    weekday_open_hour,
    weekday_close_hour,
    saturday_open_hour,
    saturday_close_hour,
    saturday_open_hour is not null      as is_open_saturday,
    sl_target,
    sl_threshold_seconds
from {{ ref('markets') }}
