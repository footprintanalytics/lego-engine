with boost_out as (
    select user,reward.tokenId,reward.chain,min(reward.day) day,reward.contract_address, -1 status from
    (
        SELECT tokenId,Date(block_timestamp) day,contract_address,user,"Ethereum" chain FROM `footprint-etl.Ethereum_izumi_autoparse.CollectReward_event_addressIndexed_uint256_address_uint256` group by 1,2,3,4
        union all
        SELECT tokenId,Date(block_timestamp) day,contract_address,user,"Polygon" chain FROM `footprint-etl.Polygon_izumi_autoparse.CollectReward_event_addressIndexed_uint256_address_uint256` group by 1,2,3,4
    ) reward left join (select token_id tokenId, 1 isBoost from `gaia-dao.gaia_dao.izumi_boost` group by token_id) boost on boost.tokenId = reward.tokenId
    where isBoost = 1
    group by tokenId,chain,contract_address,user
),
boost_in as (
    select user,tokenId,chain,contract_address,min(day) day,1 status from(
        select date(block_timestamp) as day, chain, contract_address,token_id tokenId, operator user from `gaia-dao.gaia_dao.izumi_boost`
    ) group by user,tokenId,chain,contract_address
), date_d as (

                 SELECT day from unnest(GENERATE_date_ARRAY(Date('2021-01-10 09:10:00'),current_date() , INTERVAL 1 day)) AS day

 ),
boost_in_total as (
    select *,lead(day,1,current_date()) over(partition by contract_address,chain,user order by day)  as next_day from (
        select
        day,
        chain,
        contract_address,
        user,
        sum(token_hold) over (partition by chain, contract_address,user order by day) as token_hold
        from (
            select user,date(day) as day, chain, contract_address, sum(status) as token_hold from boost_in
        group by 1,2,3,4
        )
    )

),
boost_out_total as (
    select *,lead(day,1,current_date()) over(partition by contract_address,chain,user order by day)  as next_day from (
        select
        day,
        chain,
        contract_address,
        user,
        sum(token_hold) over (partition by chain, contract_address,user order by day) as token_hold
        from (
            select user,date(day) as day, chain, contract_address, sum(status) as token_hold from boost_out
        group by 1,2,3,4
        )
    )

),
boost_out_total_daily as (
    select date_d.day,chain,user,contract_address,sum(token_hold) token_hold from boost_out_total  as la
    inner join  date_d
    on la.day<= date_d.day and la.next_day>date_d.day
    group by 1,2,3,4 order by user,day
),


boost_in_total_daily as (
    select date_d.day,chain,user,contract_address,sum(token_hold) token_hold from boost_in_total  as la
    inner join  date_d
    on la.day<= date_d.day and la.next_day>date_d.day
    group by 1,2,3,4 order by user,day
)


select day,chain,contract_address,sum(if(token_hold>0,1,0)) boost_user from (
    select day,chain,contract_address,user,sum(token_hold) token_hold from
        (
            select * from boost_out_total_daily
        union all
        select * from boost_in_total_daily
        ) t  group by 1,2,3,4 order by day
) group by 1,2,3
