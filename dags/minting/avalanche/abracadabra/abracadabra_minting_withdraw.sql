SELECT 'abracadabra'                    as project,
       '1'                              as version,
       685                              as protocol_id,
       'withdraw'                       as type,
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
         select wtd.block_timestamp,
                wtd.block_number,
                wtd.transaction_hash,
                wtd.log_index,
                rep.contract_address,
                wtd.from   as borrower,
                wtd.token  as asset_address,
                wtd.amount as asset_amount
         from footprint-etl.avalanche_abracadabra.BentoBoxV1_event_LogWithdraw as wtd
	     inner join footprint-etl.avalanche_abracadabra.CauldronV2Multichain_event_LogRepay as rep
         on wtd.transaction_hash = rep.transaction_hash

        union all

        select wtd.block_timestamp,
                wtd.block_number,
                wtd.transaction_hash,
                wtd.log_index,
                rep.contract_address,
                wtd.from   as borrower,
                wtd.token  as asset_address,
                wtd.amount as asset_amount
         from footprint-etl.avalanche_abracadabra.DegenBox_event_LogWithdraw as wtd
	     inner join footprint-etl.avalanche_abracadabra.CauldronV2Multichain_event_LogRepay as rep
         on wtd.transaction_hash = rep.transaction_hash



     ) withdraw
         LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool_info
                   ON pool_info.pool_id = withdraw.contract_address
WHERE
	Date(block_timestamp) {match_date_filter}