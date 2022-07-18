SELECT
                'maple' AS project,
                '1' AS version,
                741 AS protocol_id,
                'lending' as type,
                t.block_number AS block_number,
                t.block_timestamp AS block_time,
                t.transaction_hash AS tx_hash,
                t.log_index AS log_index,
                dep.contract_address AS contract_address,
                t.from_address AS operator,
                t.token_address AS token_address,
                CAST(t.value AS BIGNUMERIC) AS token_amount_raw,
                '' as pool_id
                FROM footprint-etl.ethereum_maple.Pool_event_DepositDateUpdated dep
                left join  footprint-blockchain-etl.crypto_ethereum.token_transfers t
                on t.from_address = dep.liquidityProvider and dep.transaction_hash = t.transaction_hash
                where Date(t.block_timestamp) {match_date_filter}