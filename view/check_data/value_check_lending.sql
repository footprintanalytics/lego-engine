WITH last_day AS (
SELECT
distinct day,address,
lead(day,1) over ( partition by address order by day ) as lastday
FROM `xed-project-237404.footprint_etl.token_daily_price`
WHERE day < CURRENT_DATE()
order by day asc
)

select
*
from last_day
where date_diff(lastday, day, day) >1
and day > '2021-01-01'