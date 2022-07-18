SELECT 'TrueFi'         AS project,
       '1'              AS version,
       71               AS protocol_id,
       'farming'        AS type,
       contract_address as pool_id,
       *
from (
         select block_number,
                block_timestamp,
                transaction_hash,
                log_index,
                contract_address,
                who                               as operator,
                token                             as asset_address,
                CAST(amountClaimed as BIGNUMERIC) as asset_amount
         from `footprint-etl.ethereum_truefi.FarmTrueFi_event_Claim`
         where Date (block_timestamp) {match_date_filter}
union all
select block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       who                               as operator,
       token                             as asset_address,
       CAST(amountClaimed as BIGNUMERIC) as asset_amount
from `footprint-etl.ethereum_truefi.StakeTrueFi_event_Claim`
where Date (block_timestamp) {match_date_filter}
    )
where asset_amount<>0 and Date (block_timestamp) {match_date_filter}