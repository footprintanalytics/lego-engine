SELECT
  dexs.block_time,
  project,
  version,
  category,
  204 as protocol_id,
  trader_b,
  token_a_amount_raw,
  token_b_amount_raw,
  NULL AS usd_amount,
  token_a_address,
  token_b_address,
  exchange_contract_address,
  tx_hash,
  trace_address,
  evt_index,
  ROW_NUMBER() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
FROM (
    -- Swapr
  SELECT
    t.block_timestamp AS block_time,
    'swapr' AS project,
    '1' AS version,
    'DEX' AS category,
    t.TO AS trader_a,
    NULL AS trader_b,
    CASE
      WHEN CAST(amount0Out AS FLOAT64) = 0 THEN amount1Out
    ELSE
    amount0Out
  END
    AS token_a_amount_raw,
    CASE
      WHEN CAST(amount0In AS FLOAT64) = 0 THEN amount1In
    ELSE
    amount0In
  END
    AS token_b_amount_raw,
    NULL AS usd_amount,
    CASE
      WHEN CAST(amount0Out AS FLOAT64) = 0 THEN f.token1
    ELSE
    f.token0
  END
    AS token_a_address,
    CASE
      WHEN CAST(amount0In AS FLOAT64) = 0 THEN f.token1
    ELSE
    f.token0
  END
    AS token_b_address,
    t.contract_address exchange_contract_address,
    t.transaction_hash AS tx_hash,
    NULL AS trace_address,
    t.log_index AS evt_index
  FROM
    footprint-etl.ethereum_swapr.DXswapPair_event_Swap t
  INNER JOIN
    footprint-etl.ethereum_swapr.DXswapFactory_event_PairCreated f
  ON
    f.pair = t.contract_address ) dexs