
        WITH source_table AS (
                SELECT 'raricapital'                                                                AS project,
       '1'                                                                          AS version,
       38                                                                           AS protocol_id,
       'liquidation'                                                                AS type,
       liquidate.block_number,
       liquidate.block_timestamp,
       liquidate.transaction_hash,
       liquidate.log_index,
       liquidate.contract_address,
       liquidate.borrower,
       liquidate.cTokenCollateral                                                   AS token_collateral_address,
       CAST(liquidate.seizeTokens AS numeric)                                       AS token_collateral_amount,
       liquidate.liquidator,
       IFNULL(transfer.token_address, '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS repay_token_address,
       CAST(liquidate.repayAmount AS numeric)                                       AS repay_token_amount
FROM `footprint-etl.ethereum_raricapital.CErc20Delegator_event_LiquidateBorrow` liquidate
         LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
                   ON (
                               transfer.transaction_hash = liquidate.transaction_hash
                           AND transfer.to_address = liquidate.contract_address
                           AND transfer.value = liquidate.repayAmount
                           AND DATE (transfer.block_timestamp) = '2021-12-05'
)
WHERE DATE (liquidate.block_timestamp) = '2021-12-05'
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
                -- 借款人
                s.borrower,
                t.symbol as token_collateral_symbol,
                s.token_collateral_address,
                s.token_collateral_amount / POW(10, t.decimals) AS token_collateral_amount,
                s.token_collateral_amount AS token_collateral_amount_raw,
                
                -- 清算人
                s.liquidator,
                tr.symbol as repay_token_symbol,
                s.repay_token_address,
                s.repay_token_amount / POW(10, tr.decimals) AS repay_token_amount,
                s.repay_token_amount AS repay_token_amount_raw,
                
                d.chain AS chain
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON LOWER(s.token_collateral_address) = LOWER(t.contract_address)
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` tr
                ON LOWER(s.repay_token_address) = LOWER(tr.contract_address)
                left join `xed-project-237404.footprint.defi_protocol_info` d
                on s.protocol_id = d.protocol_id
            )
            SELECT * FROM transactions_token
            