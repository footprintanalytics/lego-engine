SELECT
                'maple' AS project,
                '1' AS version,
                741 AS protocol_id,
                'lending' AS type,
                t.block_number AS block_number,
                t.block_timestamp AS block_time,
                t.transaction_hash AS tx_hash,
                t.log_index AS log_index,
                wd.contract_address AS contract_address,
                t.to_address AS operator,
                t.token_address AS token_address,
                CAST(t.value AS BIGNUMERIC) AS token_amount_raw,
                '' as pool_id
                FROM footprint-etl.ethereum_maple.Pool_event_FundsWithdrawn wd
                left join footprint-blockchain-etl.crypto_ethereum.token_transfers t
                on t.from_address = wd.by and t.transaction_hash = wd.transaction_hash
                where Date(t.block_timestamp) {match_date_filter}