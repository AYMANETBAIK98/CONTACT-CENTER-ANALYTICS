-- The intervals of a market and day must add up to the end-of-day report (gold.daily_market).
-- If they don't, one of the two tables lost or duplicated contacts. Returns the days that don't match.
with from_intervals as (
    select market_code, date_key, sum(contacts_offered) as offered, sum(contacts_answered) as answered
    from {{ ref('fct_interval') }}
    group by market_code, date_key
),

from_daily_report as (
    select market_code, to_char(report_date, 'YYYYMMDD')::int as date_key,
           contacts_offered as offered, contacts_answered as answered
    from {{ ref('stg_daily_market') }}
)

select *
from from_intervals i
full join from_daily_report d using (market_code, date_key)
where i.offered is distinct from d.offered
   or i.answered is distinct from d.answered
