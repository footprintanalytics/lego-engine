SELECT 'abracadabra'                                as project,
       '1'                                          as version,
       269                                          as protocol_id,
       'repay'                                      as type,
       repay.block_number,
       repay.block_timestamp                        as block_time,
       repay.transaction_hash                       as tx_hash,
       repay.log_index,
       repay.contract_address,
       pool_info.pool_id                            AS pool_id,
       borrower                                     AS operator,
       '0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3' as token_address,
       CAST(asset_amount AS BIGNUMERIC)                token_amount_raw,
FROM (
         SELECT repay.block_number AS block_number,
                repay.block_timestamp,
                repay.transaction_hash,
                repay.log_index,
                repay.contract_address,
                repay.TO           AS borrower,
                repay.amount       AS asset_amount,
         FROM `footprint-etl.ethereum_abracadabra.CauldronLowRiskV1_event_LogRepay` AS repay
                  LEFT JOIN
              `footprint-etl.ethereum_abracadabra.CauldronLowRiskV1_event_LogRemoveCollateral` remove
              ON
                          repay.transaction_hash = remove.transaction_hash
                      AND repay.log_index = remove.log_index + 1
         WHERE remove.log_index IS NULL

         union all

         SELECT repay.block_number AS block_number,
                repay.block_timestamp,
                repay.transaction_hash,
                repay.log_index,
                repay.contract_address,
                repay.TO           AS borrower,
                repay.amount       AS asset_amount
         FROM `footprint-etl.ethereum_abracadabra.CauldronV2Flat_event_LogRepay` AS repay
                  LEFT JOIN
              `footprint-etl.ethereum_abracadabra.CauldronV2Flat_event_LogRemoveCollateral` remove
              ON
                          repay.transaction_hash = remove.transaction_hash
                      AND repay.log_index = remove.log_index + 1
         WHERE remove.log_index IS NULL
     ) repay
         LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool_info
                   ON pool_info.pool_id = repay.contract_address
WHERE
    DATE (repay.block_timestamp) {match_date_filter}