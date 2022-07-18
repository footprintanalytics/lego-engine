select
pool_id
, protocol_id
, project
, chain
,business_type
,deposit_contract
,withdraw_contract
,lp_token
,stake_token
,CAST(NULL AS ARRAY<string>) AS stake_underlying_token
,reward_token
,concat(if(tokena.symbol is null  or tokena.symbol='',t.token0,tokena.symbol ),'/',if(tokenb.symbol is null  or tokena.symbol='',t.token1,tokenb.symbol)) as name
,"" as description
from
(SELECT
pair as pool_id,
16 as protocol_id,
"sushi" as project,
"Ethereum" as chain,
"dex" as business_type,
pair as deposit_contract,
pair as withdraw_contract,
pair as lp_token,
ARRAY_AGG(distinct token_b_address ) as stake_token,
CAST(NULL AS ARRAY<string>) as reward_token,
max(token0) token0,
max(token1) token1,
FROM
(select * from (
                 SELECT
             block_timestamp,
             pair,
             token0,
             token1,
             token0 as token_b_address
             FROM `blockchain-etl.ethereum_sushiswap.UniswapV2Factory_event_PairCreated`
            union all
            SELECT
            block_timestamp,
             pair,
             token0,
             token1,
             token1 as token_b_address
             FROM `blockchain-etl.ethereum_sushiswap.UniswapV2Factory_event_PairCreated`
             )
             WHERE
                DATE(block_timestamp){match_date_filter}
)group by pair
) t
left join `xed-project-237404.footprint_etl.erc20_tokens` tokena
on Lower(tokena.contract_address) = Lower(t.token0)
left join `xed-project-237404.footprint_etl.erc20_tokens` tokenb
on Lower(tokenb.contract_address) = Lower(t.token1)