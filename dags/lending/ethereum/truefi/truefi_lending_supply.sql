
                SELECT
                'truefi' AS project,
                '1' AS version,
                71 AS protocol_id,
                'lending' AS type,
                block_number,
                block_timestamp as block_time,
                transaction_hash as tx_hash,
                log_index,
                contract_address,
                borrower as operator,
                asset_address as token_address,
                asset_amount as token_amount_raw,
                '' as pool_id
                from (
                    select j.block_number,j.block_timestamp,j.transaction_hash,j.log_index,j.contract_address,staker borrower,CAST(f.value AS BIGNUMERIC)  AS asset_amount,f.token_address AS asset_address
                    from `footprint-etl.ethereum_truefi.LendingPools_event_Joined` j
                    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` f
                    using(transaction_hash)
                    where Date(f.block_timestamp) {match_date_filter}
                    and f.to_address=j.contract_address and f.from_address=j.staker
                )
            