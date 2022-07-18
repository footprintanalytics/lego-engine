SELECT 
 pool_id
, protocol_id
, project
,"Ethereum" AS chain
,business_type
,contract_address  as deposit_contract
,contract_address  as withdraw_contract
,lp_token
,stake_token
,CAST(NULL AS ARRAY<string>) AS stake_underlying_token
,reward_token
,mix.name 
,mix.description
from
(select 
    pool_id,lp_token , t.contract_address, 12 As protocol_id,'Balancer' AS project,business_type,
    ARRAY_AGG(distinct t.tokenIn IGNORE NULLS) as stake_token, CAST(NULL AS ARRAY<string>) reward_token,
    STRING_AGG(distinct if(token.symbol is null,token.contract_address,token.symbol),"/") as name,
--     '' as name,
    '' AS description
FROM 
(   -- V1
select contract_address,tokenIn,contract_address as pool_id,"dex"  as business_type,contract_address AS lp_token from
    (select pool from `blockchain-etl.ethereum_balancer.BFactory_event_LOG_NEW_POOL` where Date(block_timestamp) {match_date_filter}) newpool left join
    `blockchain-etl.ethereum_balancer.BPool_event_LOG_SWAP` t1
     on newpool.pool=t1.contract_address

--     union all
    -- V2
--     select contract_address,tokenIn,poolId as pool_id,"trade"  as business_type,null AS lp_token from `blockchain-etl.ethereum_balancer.V2_Vault_event_Swap` t2 where Date(t2.block_timestamp) {match_date_filter}
)t
left join `xed-project-237404.footprint_etl.erc20_tokens` token 
on Lower(token.contract_address)=Lower(t.tokenIn)
group by pool_id,contract_address,lp_token ,business_type 
)mix 