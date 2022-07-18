SELECT  'Convex'                              AS project,
        '1'                                   AS version,
        230                                   AS protocol_id,
        'deposit'                             AS type,
        deposit.block_number,
        deposit.block_timestamp,
        deposit.transaction_hash,
        deposit.log_index,
        deposit.contract_address,
        deposit.operator,
        deposit.asset_address,
        CAST(deposit.asset_amount AS FLOAT64) AS asset_amount,
        deposit.pool_id
FROM
(
    -- 🔒 CVX
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            _user                                        AS operator,
            '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b' AS asset_address,
            _lockedAmount                                AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Locked_CVX_event_Staked`

    UNION ALL

    -- 质押 CVX
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user                                         AS operator,
            '0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b' AS asset_address,
            amount                                       AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Staked_CVX_event_Staked`

    UNION ALL

    -- 先将 CRV 转换为 cvxCRV，然后质押 cvxCRV
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user                                         AS operator,
            '0x62b9c7356a2dc64a1969e19c23e4f579f9810aa7' AS asset_address,
            amount                                       AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Staked_cvxCRV_event_Staked`
) deposit
WHERE Date(deposit.block_timestamp) {match_date_filter}

UNION ALL

SELECT  'Convex'                              AS project,
        '1'                                   AS version,
        230                                   AS protocol_id,
        'deposit'                             AS type,
        deposit.block_number,
        deposit.block_timestamp,
        deposit.transaction_hash,
        deposit.log_index,
        deposit.contract_address,
        deposit.operator,
        transfer.token_address                AS asset_address,
        CAST(deposit.asset_amount AS FLOAT64) AS asset_amount,
        -- 用 LP 区分 pool
        transfer.token_address                AS pool_id
FROM
(
    -- 质押 SushiSwap 的 LP
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user   AS operator,
            amount AS asset_amount
    FROM `footprint-etl.ethereum_convex.Sushi_LP_event_Deposit`

    UNION ALL

    -- 质押 Curve 的 LP
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user   AS operator,
            amount AS asset_amount
    FROM `footprint-etl.ethereum_convex.Curve_Pools_Deposit_event_Deposited`
) deposit
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  transfer.transaction_hash = deposit.transaction_hash
  AND transfer.from_address = deposit.operator
  AND transfer.value = deposit.asset_amount
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE Date(deposit.block_timestamp) {match_date_filter}
