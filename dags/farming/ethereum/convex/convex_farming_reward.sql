SELECT  'Convex'                             AS project,
        '1'                                  AS version,
        230                                  AS protocol_id,
        'reward'                             AS type,
        reward.block_number,
        reward.block_timestamp,
        reward.transaction_hash,
        reward.log_index,
        reward.contract_address,
        reward.operator,
        reward.asset_address,
        CAST(reward.asset_amount AS FLOAT64) AS asset_amount,
        reward.pool_id
FROM
(
    -- 🔒 CVX 获得 cvxCRV 奖励
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            _user            AS operator,
            _rewardsToken    AS asset_address,
            _reward          AS asset_amount,
            contract_address AS pool_id
    FROM `footprint-etl.ethereum_convex.Locked_CVX_event_RewardPaid`

    UNION ALL

    -- 质押 CVX 获得 cvxCRV 奖励
    SELECT  block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            user                                         AS operator,
            '0x62b9c7356a2dc64a1969e19c23e4f579f9810aa7' AS asset_address,
            reward                                       AS asset_amount,
            contract_address                             AS pool_id
    FROM `footprint-etl.ethereum_convex.Staked_CVX_event_RewardPaid`
) reward
WHERE Date(reward.block_timestamp) {match_date_filter}

UNION ALL

-- 质押 SushiSwap 的 LP 获得 CVX + SLP 奖励
SELECT  'Convex'                                     AS project,
        '1'                                          AS version,
        230                                          AS protocol_id,
        'reward'                                     AS type,
        reward.block_number,
        reward.block_timestamp,
        reward.transaction_hash,
        reward.log_index,
        reward.contract_address,
        reward.user                                  AS operator,
        transfer.token_address                       AS asset_address,
        CAST(transfer.value AS FLOAT64)              AS asset_amount,
        -- 用 LP 区分 pool
        lp.lpToken                                   AS pool_id
FROM `footprint-etl.ethereum_convex.Sushi_LP_event_RewardPaid` reward
LEFT JOIN `footprint-etl.ethereum_convex.pid_lp_token` lp
ON lp.pid = reward.pid
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  -- TODO https://etherscan.io/tx/0xefac8b973ab0416d90b752439089f0c33c11ee1231665dcb90e18bccbff451f1
  transfer.transaction_hash = reward.transaction_hash
  AND transfer.to_address = reward.user
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE (
  transfer.value != '0'
  AND Date(reward.block_timestamp) {match_date_filter}
)

UNION ALL

-- 质押 cvxCRV 获得 CVX + CRV + 3Crv 奖励
SELECT  'Convex'                                     AS project,
        '1'                                          AS version,
        230                                          AS protocol_id,
        'reward'                                     AS type,
        reward.block_number,
        reward.block_timestamp,
        reward.transaction_hash,
        reward.log_index,
        reward.contract_address,
        reward.user                                  AS operator,
        transfer.token_address                       AS asset_address,
        CAST(transfer.value AS FLOAT64)              AS asset_amount,
        contract_address                             AS pool_id
FROM `footprint-etl.ethereum_convex.Staked_cvxCRV_event_RewardPaid` reward
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  -- TODO https://etherscan.io/tx/0x2cb972d9d435fe5d73df71aff0c1244dab5541e6f70713a5ab88b32ff2203764
  transfer.transaction_hash = reward.transaction_hash
  AND transfer.to_address = reward.user
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE Date(reward.block_timestamp) {match_date_filter}

UNION ALL

-- 质押 Curve 的 LP 获得 CVX + CRV 奖励
SELECT  'Convex'                                     AS project,
        '1'                                          AS version,
        230                                          AS protocol_id,
        'reward'                                     AS type,
        reward.block_number,
        reward.block_timestamp,
        reward.transaction_hash,
        reward.log_index,
        reward.contract_address,
        reward.user                                  AS operator,
        transfer.token_address                       AS asset_address,
        CAST(transfer.value AS FLOAT64)              AS asset_amount,
        -- 用 token 区分 pool
        transfer.token_address                       AS pool_id
FROM (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_hash) row_number
    FROM `footprint-etl.ethereum_convex.Curve_Pools_Rewards_event_RewardPaid`
  ) WHERE row_number = 1
) reward
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  -- TODO https://etherscan.io/tx/0xd8374fe369a59721547dcc73fac667e268ed76cf51ac089660198e702aa65962
  transfer.transaction_hash = reward.transaction_hash
  AND transfer.to_address = reward.user
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE Date(reward.block_timestamp) {match_date_filter}
