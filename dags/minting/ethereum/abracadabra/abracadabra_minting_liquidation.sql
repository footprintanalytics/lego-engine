SELECT 'abracadabra'                                   AS project,
       '1'                                             AS version,
       269                                             AS protocol_id,
       'liquidation'                                   AS type,
       lq.block_number                                 AS block_number,
       lq.block_timestamp                              AS block_time,
       lq.transaction_hash                             AS tx_hash,
       lq.log_index                                    AS log_index,
       lq.contract_address                             AS contract_address,
       borrower,
       token_collateral_address                        AS token_collateral_address,
       CAST(token_collateral_amount_raw AS BIGNUMERIC) AS token_collateral_amount_raw,
       tx.from_address                                 AS liquidator,
       repay_token_address                             AS repay_token_address,
       CAST(repay_token_amount_raw AS BIGNUMERIC)      AS repay_token_amount_raw,
       pool_id
FROM (
         ---- CauldronV1 --
         SELECT remove.block_number                          AS block_number,
                remove.block_timestamp                       AS block_timestamp,
                remove.transaction_hash                      AS transaction_hash,
                remove.log_index                             AS log_index,
                remove.contract_address                      AS contract_address,
                remove.from                                  AS borrower,
                pool_info.stake_token[OFFSET(0)]             AS token_collateral_address,
                remove.share                                 AS token_collateral_amount_raw,
                '0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3' AS repay_token_address,
                repay.part                                   AS repay_token_amount_raw,
                pool_info.pool_id                            AS pool_id
         FROM `footprint-etl.ethereum_abracadabra.CauldronLowRiskV1_event_LogRemoveCollateral` remove
                  LEFT JOIN
              `footprint-etl.ethereum_abracadabra.CauldronLowRiskV1_event_LogRepay` repay
              ON repay.transaction_hash = remove.transaction_hash AND repay.log_index = remove.log_index + 1
                  LEFT JOIN
              `footprint-etl.footprint_pool_infos.pool_infos` pool_info
              ON pool_info.pool_id = remove.contract_address
         WHERE repay.part IS NOT NULL
           AND DATE (remove.block_timestamp) {match_date_filter}

        UNION ALL

        ---- CauldronV2 --
        SELECT remove.block_number                          AS block_number,
               remove.block_timestamp                       AS block_timestamp,
               remove.transaction_hash                      AS transaction_hash,
               remove.log_index                             AS log_index,
               remove.contract_address                      AS contract_address,
               remove.from                                  AS borrower,
               pool_info.stake_token[OFFSET(0)]             AS token_collateral_address,
               remove.share                                 AS token_collateral_amount_raw,
               '0x99d8a9c45b2eca8864373a26d1459e3dff1e17f3' AS repay_token_address,
               repay.part                                   AS repay_token_amount_raw,
               pool_info.pool_id                            AS pool_id
        FROM `footprint-etl.ethereum_abracadabra.CauldronV2Flat_event_LogRemoveCollateral` remove
                 LEFT JOIN
             `footprint-etl.ethereum_abracadabra.CauldronV2Flat_event_LogRepay` repay
             ON repay.transaction_hash = remove.transaction_hash AND repay.log_index = remove.log_index + 1
                 LEFT JOIN
             `footprint-etl.footprint_pool_infos.pool_infos` pool_info
             ON pool_info.pool_id = remove.contract_address
        WHERE repay.part IS NOT NULL
          AND DATE (remove.block_timestamp) {match_date_filter}
    ) lq
    LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` tx
ON tx.hash = lq.transaction_hash
WHERE DATE (lq.block_timestamp) {match_date_filter}