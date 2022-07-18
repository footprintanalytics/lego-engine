with boost_out as (
    select tokenId,reward.chain,ifnull(-sum(usd_value),0) usd_value,reward.day,reward.contract_address,sum(ifnull(-token_amount,0)) token_amount from
(
    SELECT tokenId,Date(block_timestamp) day,contract_address,user,"Ethereum" chain FROM `footprint-etl.Ethereum_izumi_autoparse.CollectReward_event_addressIndexed_uint256_address_uint256` group by 1,2,3,4
    union all
    SELECT tokenId,Date(block_timestamp) day,contract_address,user,"Polygon" chain FROM `footprint-etl.Polygon_izumi_autoparse.CollectReward_event_addressIndexed_uint256_address_uint256` group by 1,2,3,4
) reward left join `gaia-dao.gaia_dao.izumi_boost` boost on safe_cast(reward.tokenId as float64) = boost.token_id and reward.chain = boost.chain and date(block_timestamp)=reward.day
group by tokenId,chain,reward.day,contract_address
),
 date_d as (

                 SELECT day from unnest(GENERATE_date_ARRAY(Date('2021-01-10 09:10:00'),current_date() , INTERVAL 1 day)) AS day

 ), boost_tvl_out as (
     select day,chain,contract_address,usd_value ,token_amount,
    lead(day,1,current_date()) over(partition by contract_address,chain order by day)  as next_day
    from
    (select
    day,
    chain,
    contract_address,
    sum(usd_value) over (partition by chain, contract_address order by day) as usd_value,
    sum(token_amount) over (partition by chain, contract_address order by day) as token_amount
    from (
        select date(day) as day, chain, contract_address, sum(usd_value) as usd_value, sum(token_amount) token_amount from boost_out
    group by 1,2,3
    )
    )
 ),
boost_tvl_in as (
    select day,chain,contract_address,usd_value ,token_amount,
    lead(day,1,current_date()) over(partition by contract_address,chain order by day)  as next_day
    from
    (select
    day,
    chain,
    contract_address,
    sum(usd_value) over (partition by chain, contract_address order by day) as usd_value,
    sum(token_amount) over (partition by chain, contract_address order by day) as token_amount
    from (
        select date(block_timestamp) as day, chain, contract_address, sum(usd_value) as usd_value, sum(token_amount) as token_amount from `gaia-dao.gaia_dao.izumi_boost`
    group by 1,2,3
    )
    )
)


select date_d.day,chain,contract_address,sum(usd_value) tvl, sum(token_amount) boost_amount from (
    select * from boost_tvl_out
    union all
    select * from boost_tvl_in
) as la
 inner join  date_d
 on la.day<= date_d.day and la.next_day>date_d.day
 group by 1,2,3 order by 1 ,3
