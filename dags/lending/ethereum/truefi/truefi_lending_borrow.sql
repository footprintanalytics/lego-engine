SELECT 'truefi' AS project,
       version,
       71       AS protocol_id,
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
         select t.version,
                t.block_number,
                t.block_timestamp,
                t.transaction_hash,
                t.log_index,
                t.contract_address,
                beneficiary                    borrower,
                CAST(f.value AS BIGNUMERIC) AS asset_amount,
                f.token_address             AS asset_address
         from (
                  select '2' AS version,
                         block_number,
                         block_timestamp,
                         transaction_hash,
                         log_index,
                         contract_address,
                         beneficiary
                  from `footprint-etl.ethereum_truefi.LoanToken2_event_Withdrawn`
                  union all
                  select '1' AS version,
                         block_number,
                         block_timestamp,
                         transaction_hash,
                         log_index,
                         contract_address,
                         beneficiary
                  from `footprint-etl.ethereum_truefi.LoanToken1_event_Withdrawn`
              ) t
                  left join `footprint-blockchain-etl.crypto_ethereum.token_transfers` f
                            using (transaction_hash)
         where Date (f.block_timestamp) {match_date_filter}
                    and f.to_address=t.beneficiary
                )
            