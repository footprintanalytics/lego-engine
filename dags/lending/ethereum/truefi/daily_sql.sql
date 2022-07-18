
            WITH source_table AS (
                
                SELECT
                'truefi' AS project,
                '2' AS version,
                71 AS protocol_id,
                'borrow' AS type,
                block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                contract_address,
                borrower,
                asset_address,
                asset_amount
                from (
                    select t.block_number,t.block_timestamp,t.transaction_hash,t.log_index,t.contract_address,beneficiary borrower,CAST(f.value AS BIGNUMERIC)  AS asset_amount,f.token_address AS asset_address
                    from `footprint-etl.ethereum_truefi.LoanToken2_event_Withdrawn` t
                    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` f
                    using(transaction_hash)
                    where Date(f.block_timestamp) = '2021-11-03'
                    and f.to_address=t.beneficiary
                )
            
            ),
            -- 连 tokoen 表
            transactions_token AS(
                SELECT
                s.type,
                s.project,
                s.version,
                s.protocol_id,
                d.protocol_slug,
                s.block_number,
                s.block_timestamp,
                s.transaction_hash,
                s.log_index,
                s.contract_address,
                s.borrower,
                t.symbol as asset_symbol,
                s.asset_address,
                asset_amount / POW(10, t.decimals) AS token_amount,
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON s.asset_address = t.contract_address
                left join `xed-project-237404.footprint.defi_protocol_info` d
                on s.protocol_id = d.protocol_id
            )
            SELECT * FROM transactions_token
            