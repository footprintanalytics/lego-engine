
                SELECT
                'truefi' AS project,
                 version,
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
                    select e.version ,e.block_number,e.block_timestamp,e.transaction_hash,e.log_index,e.contract_address, borrower,CAST(f.value AS BIGNUMERIC)  AS asset_amount,f.token_address AS asset_address
                    from
                    (select case when contract_address='0xa1e72267084192db7387c8cc1328fade470e4149' then '1' else '2' end version,block_number,block_timestamp,transaction_hash,log_index,contract_address,staker borrower from `footprint-etl.ethereum_truefi.LendingPools_event_Exited`
                    union all
                    select '2' version,block_number,block_timestamp,transaction_hash,log_index,contract_address,receiver borrower from `footprint-etl.ethereum_truefi.LoanToken2_event_Redeemed`
                    union all
                    select '1' version,block_number,block_timestamp,transaction_hash,log_index,contract_address,receiver borrower from `footprint-etl.ethereum_truefi.LoanToken1_event_Redeemed`
                    )e
                    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` f
                    using(transaction_hash)
                    left join (select t1.hash,t1.from_address from `footprint-blockchain-etl.crypto_ethereum.transactions` t1 where Date(t1.block_timestamp) {match_date_filter})et
                    on et.hash=f.transaction_hash
                    where Date(f.block_timestamp) {match_date_filter}
                    and f.to_address=e.borrower and f.from_address=e.contract_address
                    and e.borrower=et.from_address
                )
            