-- One row per agent: the market they work for and when we first / last saw them scheduled.
select
    agent_username,
    min(market_code)                             as market_code,        -- an agent works for one market
    min(work_date)                               as first_scheduled_date,
    max(work_date)                               as last_scheduled_date,
    count(*)                                     as days_scheduled,
    count(*) filter (where is_absent)            as days_absent
from {{ ref('stg_agent_daily') }}
group by agent_username
