
with
vaults as (
    SELECT * FROM UNNEST(
        ARRAY_CONCAT(
            ARRAY(SELECT vault FROM `footprint-etl.ethereum_yearn.NewExperimentalVault_event_NewExperimentalVault`),
            [
            '0x671a912c10bba0cfa74cfc2d6fba9ba1ed9530b2',
            '0x986b4aff588a109c09b50a03f42e4110e29d353f',
            '0xa696a63cc78dffa1a63e9e50587c197387ff6c7e',
            '0xe11ba472f74869176652c35d30db89854b5ae84d',
            '0xcb550a6d4c8e3517a939bc79d0c7093eb7cf56b5',
            '0xbfa4d8aa6d8a379abfe7793399d3ddacc5bbecbb',
            '0xe2f6b9773bf3a015e2aa70741bde1498bdb9425b'
            ]
        )
    )
),
transactions as (
    SELECT `hash`,block_number,block_timestamp,to_address, from_address, transaction_index FROM `footprint-blockchain-etl.crypto_ethereum.transactions`
    WHERE DATE(block_timestamp) {match_date_filter}
    AND receipt_status = 1
    AND SUBSTR(input, 1, 10) IN ('0x2e1a7d4d', '0x3ccfd60b', '0x00f714ce', '0x853828b6')
    AND to_address in (SELECT * FROM vaults)
),

tokenTransfer as (
    SELECT transaction_hash, token_address, value, from_address, to_address FROM `footprint-blockchain-etl.crypto_ethereum.token_transfers`
    WHERE DATE(block_timestamp) {match_date_filter}
    AND from_address in (SELECT * FROM vaults)
)

SELECT
    'Yearn Finance' AS project,
    '2' AS version,
    9 AS protocol_id,
    'withdraw' AS type,
    *
from (
    SELECT
        transactions.block_number,
        transactions.block_timestamp,
        `hash` AS transaction_hash,
        transaction_index AS log_index,
        transactions.to_address AS contract_address,
        transactions.from_address AS operator,
        tokenTransfer.token_address AS asset_address,
        CAST(tokenTransfer.value as BIGNUMERIC) AS asset_amount,
        transactions.to_address AS pool_id
    FROM transactions
    LEFT JOIN tokenTransfer
    ON (transactions.`hash` = tokenTransfer.transaction_hash and transactions.from_address = tokenTransfer.to_address and transactions.from_address = tokenTransfer.to_address)
)
where DATE(block_timestamp) {match_date_filter}

