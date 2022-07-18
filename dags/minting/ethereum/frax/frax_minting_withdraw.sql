
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
        f.block_timestamp,
        f.block_number,
        f.transaction_hash,
        f.transaction_index as log_index,
        t.from_address as contract_address,
        t.to_address as borrower,
        t.token_address as asset_address,
        t.value as asset_amount
    from `footprint-etl.ethereum_frax.Pool_USDC_call_collectRedemption` f
    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` t
    on f.transaction_hash = t.transaction_hash
    where Date(t.block_timestamp) {match_date_filter}
    and t.from_address = '0x1864ca3d47aab98ee78d11fc9dcc5e7badda1c0d'
    and t.token_address in ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', '0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0')
    and f.status = 1

    union all

    select
        f.block_timestamp,
        f.block_number,
        f.transaction_hash,
        f.transaction_index as log_index,
        t.from_address as contract_address,
        t.to_address as borrower,
        t.token_address as asset_address,
        t.value as asset_amount
    from `footprint-etl.ethereum_frax.Old_Pool_USDC_call_collectRedemption` f
    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` t
    on f.transaction_hash = t.transaction_hash
    where Date(t.block_timestamp) {match_date_filter}
    and t.from_address = '0x3c2982ca260e870eee70c423818010dfef212659'
    and t.token_address in ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', '0x3432b6a60d23ca0dfca7761b7ab56459d9c964d0')
    and f.status = 1
)
where Date(block_timestamp) {match_date_filter}
            