SELECT 'abracadabra'                    as project,
       '1'                              as version,
       685                              as protocol_id,
       'supply'                         as type,
       block_number,
       block_timestamp                  AS block_time,
       transaction_hash                 AS tx_hash,
       log_index,
       contract_address,
       borrower                         AS operator,
       asset_address                    AS token_address,
       CAST(asset_amount AS BIGNUMERIC) AS token_amount_raw,
       pool_info.pool_id                AS pool_id
FROM (
         select dep.block_timestamp,
                dep.block_number,
                dep.transaction_hash,
                dep.log_index,
                bor.contract_address,
                dep.from   as borrower,
                dep.token  as asset_address,
                dep.amount as asset_amount
         from footprint-etl.avalanche_abracadabra.BentoBoxV1_event_LogDeposit as dep
	     inner join footprint-etl.avalanche_abracadabra.CauldronV2Multichain_event_LogBorrow as bor
         on dep.transaction_hash = bor.transaction_hash

        union all

        select dep.block_timestamp,
                dep.block_number,
                dep.transaction_hash,
                dep.log_index,
                bor.contract_address,
                dep.from   as borrower,
                dep.token  as asset_address,
                dep.amount as asset_amount
         from footprint-etl.avalanche_abracadabra.DegenBox_event_LogDeposit as dep
	     inner join footprint-etl.avalanche_abracadabra.CauldronV2Multichain_event_LogBorrow as bor
         on dep.transaction_hash = bor.transaction_hash
     ) supply
         LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool_info
                   ON pool_info.pool_id = supply.contract_address

WHERE
	Date(block_timestamp) {match_date_filter}