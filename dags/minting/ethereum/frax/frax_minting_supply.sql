
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
        t.block_timestamp,
        t.block_number,
        t.transaction_hash,
        t.log_index,
        f.to_address as contract_address,
        t.from as borrower,
        t.contract_address as asset_address,
        t.amount as asset_amount
    from `footprint-etl.ethereum_frax.FRAXShares_event_FXSBurned` t
    left join `footprint-blockchain-etl.crypto_ethereum.transactions` f
    on t.transaction_hash = f.hash
    where Date(f.block_timestamp) {match_date_filter}

    union all

    select
        f.block_timestamp,
        f.block_number,
        f.transaction_hash,
        f.log_index,
        t.to_address as contract_address,
        f.from as borrower,
        t.token_address as asset_address,
        t.value as asset_amount
    from `footprint-etl.ethereum_frax.FRAXShares_event_FXSBurned` f
    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` t
    on f.transaction_hash = t.transaction_hash
    where t.token_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    and Date(t.block_timestamp) {match_date_filter}
 )
where Date(block_timestamp) {match_date_filter}
