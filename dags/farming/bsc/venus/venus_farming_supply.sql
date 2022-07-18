SELECT
    'Venus' as project,
    '1' as version,
    118 as protocol_id,
    'supply' as type,
    a.block_number,
    a.block_timestamp,
    a.transaction_hash,
    a.log_index,
    a.contract_address,
    a.user as operator,
    b.token_address as asset_address,
    cast(a.amount as BIGNUMERIC) as asset_amount,
    a.contract_address as pool_id
from (
    select * from `footprint-etl.bsc_venus.VAIVault_event_Deposit`
    where amount != '0'
    union all
    select block_timestamp, block_number, transaction_hash, log_index, contract_address, user, amount from `footprint-etl.bsc_venus.XVSVault_event_Deposit`
) a
left join (
    select * from `footprint-blockchain-etl.crypto_bsc.token_transfers`
    where Date(block_timestamp) {match_date_filter}
) b
on a.transaction_hash = b.transaction_hash and b.from_address = a.user and b.to_address = a.contract_address
and a.amount = b.value

where Date(a.block_timestamp) {match_date_filter}
            