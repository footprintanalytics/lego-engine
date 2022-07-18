    -- swap v1
SELECT
    t.block_timestamp AS block_time,
    'Balancer' AS project,
    '1' AS version,
    'DEX' AS category,
    12 As protocol_id,
    null AS trader_a,
    null AS trader_b,
    CAST(t.tokenAmountOut AS  FLOAT64) AS token_a_amount_raw,
    CAST(t.tokenAmountIn AS FLOAT64) AS token_b_amount_raw,
    null AS usd_amount,
    t.tokenOut token_a_address,
    t.tokenIn token_b_address,
    t.contract_address exchange_contract_address,
    t.transaction_hash AS tx_hash,
    null AS trace_address
FROM `blockchain-etl.ethereum_balancer.BPool_event_LOG_SWAP` t

UNION ALL

-- swap v2
SELECT
    t.block_timestamp AS block_time,
    'Balancer' AS project,
    '2' AS version,
    'DEX' AS category,
    12 As protocol_id,
    null AS trader_a,
    null AS trader_b,
    CAST(t.amountOut AS  FLOAT64) AS token_a_amount_raw,
    CAST(t.amountIn AS  FLOAT64) AS token_b_amount_raw,
    null AS usd_amount,
    t.tokenOut AS token_a_address,
    t.tokenIn AS token_b_address,
    t.contract_address AS exchange_contract_address,
    t.transaction_hash AS tx_hash,
    null AS trace_address
FROM `blockchain-etl.ethereum_balancer.V2_Vault_event_Swap` t
