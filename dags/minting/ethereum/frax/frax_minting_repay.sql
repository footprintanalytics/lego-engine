
SELECT
    'Frax' as project,
    '1' as version,
    280 as protocol_id,
    'minting' as type,
    block_number,
    block_timestamp as block_time,
    transaction_hash as tx_hash,
    log_index,
    contract_address,
    borrower as operator,
    asset_address as token_address,
    CAST(asset_amount as numeric) as token_amount_raw,
     '' as pool_id
from (
    select
        block_timestamp,
        block_number,
        transaction_hash,
        log_index,
        t.to as contract_address,
        t.from as borrower,
        contract_address as asset_address,
        amount as asset_amount
    from `footprint-etl.ethereum_frax.FRAXStablecoin_event_FRAXBurned` t
)
where Date(block_timestamp) {match_date_filter}
            