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
                --还款动作Settled与用户实际的tokenTransfers操作在不同的流水中，而需要用到tokenTransfers的流水来解析出还款金额，在这里就有一个合约地址选哪个的问题（loanToken还是token？）
                --我这里暂时先选loanToken(贷款合约,例子：0xb6AA9AB4555A4a5FE1e111C6A9330aD07D8C98fd)
                    select s.version,s.block_number,s.block_timestamp,s.transaction_hash,s.log_index,s.contract_address,beneficiary borrower,CAST(f.value AS BIGNUMERIC) AS asset_amount,token_address AS asset_address
                    from (
                        select '2' AS version,block_number,block_timestamp,transaction_hash,log_index,contract_address from `footprint-etl.ethereum_truefi.LoanToken2_event_Settled`
                        union all
                        select '1' AS version,block_number,block_timestamp,transaction_hash,log_index,contract_address from `footprint-etl.ethereum_truefi.LoanToken1_event_Closed`
                    ) s
                    left join (
                        select '2' AS version,contract_address,beneficiary from `footprint-etl.ethereum_truefi.LoanToken2_event_Withdrawn`
                        union all
                        select '1' AS version,contract_address,beneficiary from `footprint-etl.ethereum_truefi.LoanToken1_event_Withdrawn`
                    ) t
                    on s.contract_address=t.contract_address and s.version=t.version
                    left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` f
                    on f.from_address=t.beneficiary and f.to_address=s.contract_address
                    where Date(f.block_timestamp) {match_date_filter}
                )
            