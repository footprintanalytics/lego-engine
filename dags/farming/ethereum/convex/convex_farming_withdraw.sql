SELECT  'Convex'                               AS project,
        '1'                                    AS version,
        230                                    AS protocol_id,
        'withdraw'                             AS type,
        withdraw.block_number,
        withdraw.block_timestamp,
        withdraw.transaction_hash,
        withdraw.log_index,
        withdraw.contract_address,
        withdraw.operator,
        withdraw.asset_address,
        CAST(withdraw.asset_amount AS FLOAT64) AS asset_amount,
        withdraw.pool_id
FROM
(
    -- 🔓 CVX（截止到 2021.12.16 还没有人解锁）
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            _user                                        AS operator,
            '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b' AS asset_address,
            _amount                                      AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Locked_CVX_event_Withdrawn`

    UNION ALL

    -- 赎回 CVX
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user                                         AS operator,
            '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b' AS asset_address,
            amount                                       AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Staked_CVX_event_Withdrawn`

    UNION ALL

    -- 赎回 cvxCRV
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user                                         AS operator,
            '0x62b9c7356a2dc64a1969e19c23e4f579f9810aa7' AS asset_address,
            amount                                       AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Staked_cvxCRV_event_Withdrawn`
) withdraw
WHERE Date(withdraw.block_timestamp) {match_date_filter}

UNION ALL

SELECT  'Convex'                               AS project,
        '1'                                    AS version,
        230                                    AS protocol_id,
        'withdraw'                             AS type,
        withdraw.block_number,
        withdraw.block_timestamp,
        withdraw.transaction_hash,
        withdraw.log_index,
        withdraw.contract_address,
        withdraw.operator,
        transfer.token_address                 AS asset_address,
        CAST(withdraw.asset_amount AS FLOAT64) AS asset_amount,
        -- 用 LP 区分 pool
        transfer.token_address                 AS pool_id
FROM
(
    -- 赎回 SushiSwap 的 LP
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user   AS operator,
            amount AS asset_amount
    FROM `footprint-etl.ethereum_convex.Sushi_LP_event_Withdraw`

    UNION ALL

    -- 赎回 Curve 的 LP
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user   AS operator,
            amount AS asset_amount
    FROM `footprint-etl.ethereum_convex.Curve_Pools_Deposit_event_Withdrawn`
) withdraw
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  transfer.transaction_hash = withdraw.transaction_hash
  AND transfer.to_address = withdraw.operator
  AND transfer.value = withdraw.asset_amount
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE Date(withdraw.block_timestamp) {match_date_filter}
