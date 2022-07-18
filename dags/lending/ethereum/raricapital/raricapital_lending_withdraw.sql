
                SELECT
                'raricapital' AS project,
                '1' AS version,
                38 AS protocol_id,
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
                select b.block_number,b.block_timestamp,b.transaction_hash,b.log_index,b.contract_address,b.redeemer borrower,CAST(f.value AS BIGNUMERIC)  AS asset_amount,f.token_address AS asset_address
                from`footprint-etl.ethereum_raricapital.CErc20Delegator_event_Redeem` b
                left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` f
                on f.transaction_hash=b.transaction_hash and b.redeemer=f.to_address and f.from_address=b.contract_address and f.value=b.redeemAmount
                where Date(f.block_timestamp) {match_date_filter}
                )
            