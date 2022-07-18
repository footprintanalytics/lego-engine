with cashflow as (
    SELECT
        DATE(block_timestamp) day,
        protocol_slug,
        pool_id,
        contract_address,
        business_type,
        pool_name,
        chain,
        SUM(ifnull(usd_value,0)) AS current_value
    FROM `gaia-data.gaia.defi_transactions_90d`
    group by 1,2,3,4,5,6,7
),
pool as (
    select
        pool_id,
        defi_category
    from `gaia-data.gaia.defi_pool_info`
    where defi_category = 'Lending'
)

select day,
  protocol_slug,
  chain,
  pool_id,
  pool_name,
  contract_address,
  business_type,
  pool.defi_category AS type,
  'usd_value' AS indicators,
  current_value
from cashflow inner join pool using(pool_id)