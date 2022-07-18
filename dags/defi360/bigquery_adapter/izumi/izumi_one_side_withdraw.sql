with withdraw_event as (
    select mix.* from(
        select block_number,block_timestamp,transaction_hash,log_index,contract_address,safe_cast(tokenId as float64) as token_id,'erc20Withdraw' as business_type from `footprint-etl.Polygon_izumi_autoparse.Withdraw_event_addressIndexed_uint256`
        union all
        select block_number,block_timestamp,transaction_hash,log_index,contract_address,safe_cast(tokenId as float64) as token_id,'erc20Withdraw' as business_type from `footprint-etl.Ethereum_izumi_autoparse.Withdraw_event_addressIndexed_uint256`
    )mix inner join `gaia-dao.gaia_dao.gaia__custom__186_izumi_one_side_pool` pool
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
from withdraw_event as event left join  `gaia-dao.gaia_dao.izumi_one_side_supply` using(token_id,contract_address)