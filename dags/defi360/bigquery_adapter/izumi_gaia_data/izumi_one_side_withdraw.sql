with withdraw_event as (
    select mix.* from(
        select block_number,block_timestamp,transaction_hash,log_index,contract_address,safe_cast(tokenId as float64) as token_id,'erc20Withdraw' as business_type from `gaia-contract-events.izumi_polygon.parse_withdraw_event`
        union all
        select block_number,block_timestamp,transaction_hash,log_index,contract_address,safe_cast(tokenId as float64) as token_id,'erc20Withdraw' as business_type from `gaia-contract-events.izumi_ethereum.parse_withdraw_event`
        union all
        select block_number,block_timestamp,transaction_hash,log_index,contract_address,safe_cast(tokenId as float64) as token_id,'erc20Withdraw' as business_type from `gaia-contract-events.izumi_arbitrum.parse_withdraw_event`
    )mix inner join `gaia-data.gaia.izumi_one_side_pool` pool
    using(contract_address)
    where date(mix.block_timestamp) {run_date}
)
select
    project,
    chain,
    protocol_id,
    event.block_number,
    event.block_timestamp,
    event.transaction_hash,
    event.log_index,
    contract_address,
    operator,
    token_symbol,
    token_address,
    token_amount,
    safe_cast(token_amount_raw as float64) token_amount_raw,
    pool_id,
    name,
    version,
    event.business_type,
    usd_value,
    token_id
from withdraw_event as event left join  `gaia-data.gaia.izumi_one_side_supply` using(token_id,contract_address)