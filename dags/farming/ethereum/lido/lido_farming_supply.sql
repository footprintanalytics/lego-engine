with transactions as (
    select
        block_number,
        block_timestamp,
        `hash` AS transaction_hash,
        transaction_index AS log_index,
        '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' AS contract_address,
        from_address AS operator,
        '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' AS asset_address,
        from `footprint-blockchain-etl.crypto_ethereum.transactions`
        where date(block_timestamp) {match_date_filter}
        and to_address = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
        and substr(input, 1 , 10) = '0xea598cb0'
        AND receipt_status = 1

),
tokenTransfer as (
    select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers`
    where date(block_timestamp) {match_date_filter}
    and to_address = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
)

SELECT
    'Lido' AS project,
    '1' AS version,
    88 AS protocol_id,
    'supply' AS type,
    *
from (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        sender AS operator,
        'eth' AS asset_address,
        CAST(amount as BIGNUMERIC) AS asset_amount,
        contract_address as pool_id
    from  `footprint-etl.ethereum_lido.Lido_Stake_event_Submitted`

    union all

    (select
        transactions.block_number,
        transactions.block_timestamp,
        transactions.transaction_hash,
        transactions.log_index,
        contract_address,
        operator,
        asset_address,
        CAST(tokenTransfer.value as BIGNUMERIC) AS asset_amount,
        contract_address as pool_id
    from transactions
    left join tokenTransfer
    on transactions.transaction_hash = tokenTransfer.transaction_hash)
)
where DATE(block_timestamp) {match_date_filter}