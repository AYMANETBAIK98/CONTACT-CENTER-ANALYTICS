-- One row per 15-minute slot of the day, in LOCAL time: 96 rows, 00:00 to 23:45.
-- interval_key = 0 for 00:00, 1 for 00:15 ... 95 for 23:45.
-- Local time, because opening hours and peaks are local: "the 11:00 slot" means 11:00 in London for UK
-- and 11:00 in Madrid for ES.
with slots as (
    select generate_series(0, 95) as interval_key
)

select
    interval_key,
    (time '00:00' + interval_key * interval '15 minutes')::time        as slot_start,
    (time '00:00' + (interval_key + 1) * interval '15 minutes')::time  as slot_end,
    to_char(time '00:00' + interval_key * interval '15 minutes', 'HH24:MI')   as slot_label,     -- "11:15"
    interval_key / 4                                                   as hour_of_day,
    case
        when interval_key < 48 then 'Morning'          -- before 12:00
        when interval_key < 72 then 'Afternoon'        -- 12:00 - 18:00
        else 'Evening'
    end                                                                 as day_part
from slots
