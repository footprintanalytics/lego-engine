with
transfer as (
    select *, 'Ethereum' as chain from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where date(block_timestamp)  {run_date}
    union all 
    select *, 'Polygon' as chain from `footprint-blockchain-etl.crypto_polygon.token_transfers` where date(block_timestamp)  {run_date}
),
deposit_event as (
    select event.*,pool.lock_token as token_address from 
    (select * , 'Polygon' as chain from `footprint-etl.Polygon_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256`
    union all 
    select * , 'Ethereum' as chain FROM `footprint-etl.Ethereum_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256`) event
    inner join `gaia-dao.gaia_dao.gaia__custom__186_izumi_one_side_pool` pool
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
    from one_side_amount as supply left join `gaia-dao.gaia_dao.erc20_tokens_all` token using(token_address,chain)
    left join `gaia-dao.gaia_dao.token_price_daily_100d` p
    on p.token_address = supply.token_address and Date(supply.block_timestamp)=p.day and supply.chain = p.chain
),

one_side_cashflow as (
    select
    poolInfo.protocol_id
    ,poolInfo.pool_id
    ,poolInfo.name
    ,poolInfo.version
    ,usd.* from one_side_usd usd
    left join `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a_poolInfo_deposit_contract` pool_contract
    on usd.chain=pool_contract.chain and usd.contract_address = pool_contract.deposit_contract_address
    left join `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a_poolInfo` poolInfo
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