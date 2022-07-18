SELECT
    'Venus' as project,
    '1' as version,
    118 as protocol_id,
    'reward' as type,
    a.block_number,
    a.block_timestamp,
    a.transaction_hash,
    a.log_index,
    a.contract_address,
    a.user as operator,
    b.token_address as asset_address,
    CAST(b.value as BIGNUMERIC) as asset_amount,
    a.contract_address as pool_id
from (
    select * from (
        select * from `footprint-etl.bsc_venus.VAIVault_event_Withdraw`
        union all
        select * from `footprint-etl.bsc_venus.VAIVault_event_Deposit`
    )
    where amount = '0'
) a inner join (
    select * from `footprint-blockchain-etl.crypto_bsc.token_transfers`
    where Date(block_timestamp) {match_date_filter}
) b
on
    a.transaction_hash = b.transaction_hash and  a.contract_address = b.from_address and a.user = b.to_address

where Date(a.block_timestamp) {match_date_filter}

union all

SELECT
    'Venus' as project,
    '1' as version,
    118 as protocol_id,
    'reward' as type,
    d.block_number,
    d.block_timestamp,
    d.transaction_hash,
    d.log_index,
    d.contract_address,
    d.user as operator,
    d.rewardToken as asset_address,
    CAST(e.value as BIGNUMERIC) as asset_amount,
    d.contract_address as pool_id
from (
        select * from `footprint-etl.bsc_venus.XVSVault_event_Deposit`
        union all
        select * from `footprint-etl.bsc_venus.XVSVault_event_ExecutedWithdrawal`
    ) d inner join (
        select * from `footprint-blockchain-etl.crypto_bsc.token_transfers`
        where Date(block_timestamp) {match_date_filter}
    ) e
on
   d.transaction_hash = e.transaction_hash and lower(from_address) = lower('0x1e25cf968f12850003db17e0dba32108509c4359') and e.to_address = d.user
where Date(d.block_timestamp) {match_date_filter}