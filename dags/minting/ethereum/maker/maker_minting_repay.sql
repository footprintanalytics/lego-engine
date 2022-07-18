
                SELECT
                'MakerDAO' AS project,
                '2' AS version,
                15 AS protocol_id,
                'minting' AS type,
                maker.block_number As block_number,
                maker.block_timestamp As block_time,
                maker.transaction_hash As tx_hash,
                0 AS log_index,
                maker.to_address AS contract_address,
                f.from_address AS operator,
                '0x6b175474e89094c44da98b954eedeac495271d0f' AS token_address,
                asset_amount as token_amount_raw,
                 '' as pool_id
                from (
                    SELECT block_number,to_address, block_timestamp, transaction_hash, trace_address, CAST(wad AS FLOAT64) AS asset_amount, usr AS borrower
                    FROM `blockchain-etl.ethereum_maker.Dai_call_burn`
                    WHERE error IS NULL
                    AND CAST(wad AS FLOAT64) > 0
                    AND Date(block_timestamp) {match_date_filter}

                    UNION ALL

                    SELECT block_number,to_address, block_timestamp, transaction_hash, trace_address, CAST(rad AS FLOAT64)/1e27 AS asset_amount, dst AS borrower
                    FROM `blockchain-etl.ethereum_maker.Vat_call_move`
                    WHERE error IS NULL AND src = '0x197e90f9fad81970ba7976f33cbd77088e5d7cf7'
                    AND CAST(rad AS FLOAT64)>0
                    AND Date(block_timestamp) {match_date_filter}
                ) maker left join
                `footprint-blockchain-etl.crypto_ethereum.transactions` f
                on maker.transaction_hash=f.hash
                WHERE Date(f.block_timestamp) {match_date_filter}
            