with max_day as (
    select max(date(block_timestamp)) as day,protocol_slug from `gaia-dao.gaia_dao.cashflow` group by protocol_slug
),
cashflow as(
    select chain,protocol_slug,pool_id,business_type,name
,if(business_type='lendingWithdraw',ifnull(usd_value,0),0) as lendingWithdraw
,if(business_type='lendingRepay',ifnull(usd_value,0),0) as lendingRepay
,if(business_type='lendingBorrow',ifnull(usd_value,0),0) as lendingBorrow
,if(business_type='lendingSupply',ifnull(usd_value,0),0) as lendingSupply
,date(block_timestamp) as day from `gaia-dao.gaia_dao.cashflow` where business_type in('lendingWithdraw','lendingRepay','lendingBorrow','lendingSupply')
),
lastest_cashflow as (
    SELECT chain,protocol_slug,pool_id,name,day
        ,sum(lendingWithdraw) as lendingWithdraw
        ,sum(lendingRepay) as lendingRepay
        ,sum(lendingBorrow) as lendingBorrow
        ,sum(lendingSupply) as lendingSupply
    FROM
    cashflow
    inner join max_day
    using(day,protocol_slug)
    group by 1,2,3,4,5 order by pool_id
),
lastest_cashflow_tvl as (
    select lastest_cashflow.*, ifnull(pool_tvl.usd_value,0) as tvl from
    lastest_cashflow
    left join `gaia-dao.gaia_dao.pool_info` pool_info using(pool_id)
    left join
    `gaia-dao.gaia_dao.pool_tvl` as pool_tvl
    on pool_info.contract_address=pool_tvl.pool_address and lastest_cashflow.day=pool_tvl.day and lastest_cashflow.chain = pool_tvl.chain
)
select * from lastest_cashflow_tvl


