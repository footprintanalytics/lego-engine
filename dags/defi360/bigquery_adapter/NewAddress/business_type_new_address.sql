select *
from (
         select
             protocol_slug,
             business_type,
             operator as wallet_address,
             Date (Min (block_timestamp)) as first_day,
             'DeFi' as type
     from `gaia-data.gaia.defi_transactions_90d`
group by 1, 2, 3
    )
where first_day {run_date}