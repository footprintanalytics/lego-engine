with
transfer as (
    select *, 'Ethereum' as chain from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where date(block_timestamp)  {run_date}
    union all
    select *, 'Polygon' as chain from `footprint-blockchain-etl.crypto_polygon.token_transfers` where date(block_timestamp)  {run_date}
    union all
    select *, 'Arbitrum' as chain from `footprint-blockchain-etl.crypto_arbitrum.token_transfers` where date(block_timestamp)  {run_date}
),
deposit_event as (
    select event.*,pool.lock_token as token_address from
    (select * , 'Polygon' as chain from `gaia-contract-events.izumi_polygon.parse_deposit_event`
    union all
    select * , 'Ethereum' as chain FROM `gaia-contract-events.izumi_ethereum.parse_deposit_event`
    union all
    select * , 'Arbitrum' as chain FROM `gaia-contract-events.izumi_arbitrum.parse_deposit_event`
    ) event
    inner join `gaia-data.gaia.izumi_one_side_pool` pool
    on pool.contract_address = event.contract_address
    where date(event.block_timestamp)  {run_date}
),
deposit_amount as (
    select d.*,t.value token_amount_raw from deposit_event d left join transfer t
    on d.transaction_hash = t.transaction_hash
    and t.token_address = d.token_address
    and t.to_address = d.contract_address
    and t.from_address = d.user
    and t.chain = d.chain
),


one_side_amount as (
    select
        'izumi' as project,
        chain,
        'erc20Supply' as business_type,
		block_number,
		block_timestamp,
		transaction_hash,
		contract_address,
		user as operator,
        safe_cast(tokenId as float64) as token_id,
        token_amount_raw,
        log_index,
        token_address
    from deposit_amount
),
one_side_usd as (
    select  supply.*
    ,token.token_symbol
    ,safe_cast(supply.token_amount_raw as float64)* pow(10,-if(token.decimals is null,18,token.decimals)) token_amount
    ,safe_cast(supply.token_amount_raw as float64)* pow(10,-if(token.decimals is null,18,token.decimals)) * p.price as usd_value
    from one_side_amount as supply left join `gaia-data.gaia.erc20_tokens` token using(token_address,chain)
    left join `gaia-data.origin_data.token_price_daily` p
    on p.token_address = supply.token_address and Date(supply.block_timestamp)=p.on_date and supply.chain = p.chain
),

one_side_cashflow as (
    select
    poolInfo.protocol_slug as protocol_id
    ,poolInfo.pool_id
    ,poolInfo.pool_name as name
    ,poolInfo.pool_version as version
    ,usd.* from one_side_usd usd
    left join (select deposit_contract_address_list[offset(0)] as deposit_contract_address,chain,pool_id from `gaia-data.struct_data.izumi_pool_info`) pool_contract
    on usd.chain=pool_contract.chain and usd.contract_address = pool_contract.deposit_contract_address
    left join `gaia-data.struct_data.izumi_pool_info` poolInfo
    on pool_contract.pool_id= poolInfo.pool_id
)
-- 调整结构
select
    project,
    chain,
    protocol_id,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    contract_address,
    operator,
    token_symbol,
    token_address,
    token_amount,
    safe_cast(token_amount_raw as float64) token_amount_raw,
    pool_id,
    name,
    version,
    business_type,
    usd_value,
    token_id
from one_side_cashflow