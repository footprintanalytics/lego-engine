                SELECT
                'MakerDAO' AS project,
                '2' AS version,
                 15 AS protocol_id,
                'minting' AS type,
                block_number,
                block_timestamp as block_time,
                transaction_hash as tx_hash,
                log_index,
                tr.from_address as contract_address,
                et.from_address AS operator,
                tr.token_address AS token_address,
                CAST(value AS FLOAT64) AS token_amount_raw,
                 '' as pool_id
                from `footprint-blockchain-etl.crypto_ethereum.token_transfers` tr
                left join (select t.hash AS  transaction_hash,t.to_address,t.from_address from `footprint-blockchain-etl.crypto_ethereum.transactions` t where  Date(block_timestamp) {match_date_filter}) et
                using(transaction_hash)
                WHERE tr.from_address IN (SELECT address FROM `xed-project-237404.footprint_etl.makermcd_collateral_addresses`)
                --     AND DATE(block_timestamp) >= '2021-01-21'
                AND Date(block_timestamp) {match_date_filter}
            