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
,concat(if(tokena.symbol is null,t.asset_token,tokena.symbol )) as name
, description
from
(SELECT
aToken as pool_id,
6 as protocol_id,
"Aave" as project,
"Ethereum" as chain,
"lending" as business_type,
aToken as deposit_contract,
aToken as withdraw_contract,
'' as lp_token,
ARRAY_AGG(distinct asset ) as stake_token,
CAST(NULL AS ARRAY<string>) as reward_token,
max(asset) asset_token,
max(if(create_address="0xb53c1a33016b2dc2ff3653530bff1848a515c8c5","0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9"
,if(create_address="0x6fdfafb66d39cd72cfe7984d3bbcc76632faab00","0x37d7306019a38af123e4b245eb6c28af552e0bb0",
if(create_address="0xacc030ef66f9dfeae9cbb0cd1b25654b82cfa8d5","0x7937d4799803fbbe595ed57278bc4ca21f3bffcb","-")))
 ) description
from (SELECT * FROM `footprint-etl.ethereum_aave.upgradeabilityProxy_event_ReserveInitialized` a
left join(select newAddress,contract_address as create_address from `footprint-etl.ethereum_aave.LendingPoolAddressesProvider_event_ProxyCreated` )c on a.contract_address=c.newAddress
where DATE(block_timestamp){match_date_filter}
) group by aToken
)t
left join `xed-project-237404.footprint_etl.erc20_tokens` tokena
on Lower(tokena.contract_address) = Lower(t.asset_token)