-- One row per calendar day, from the first to the last day in the data.
-- date_key = 20261104 (an integer): the usual key of a date dimension, fast to join in Power BI.
with bounds as (
    select min(interval_date) as first_day, max(interval_date) as last_day
    from {{ ref('stg_interval_kpis') }}
),

days as (
    select generate_series(first_day, last_day, interval '1 day')::date as calendar_date
    from bounds
)

select
    to_char(calendar_date, 'YYYYMMDD')::int         as date_key,
    calendar_date,
    extract(isodow from calendar_date)::int         as day_of_week,      -- 1 = Monday ... 7 = Sunday
    trim(to_char(calendar_date, 'Day'))             as day_name,
    to_char(calendar_date, 'Dy')                    as day_short,
    extract(isodow from calendar_date) >= 6         as is_weekend,
    extract(week from calendar_date)::int           as iso_week,
    to_char(calendar_date, 'IYYY-"W"IW')            as iso_year_week,    -- 2026-W45
    extract(month from calendar_date)::int          as month_number,
    trim(to_char(calendar_date, 'Month'))           as month_name,
    extract(year from calendar_date)::int           as year
from days
