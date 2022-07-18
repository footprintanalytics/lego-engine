SELECT
  dexs.block_time,
  project,
  version,
  category,
  450 as protocol_id,
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
    -- Mooniswap
  SELECT
    block_timestamp AS block_time,
    'Mooniswap' AS project,
    '1' AS version,
    'DEX' AS category,
    account AS trader_a,
    NULL AS trader_b,
    result AS token_a_amount_raw,
    amount AS token_b_amount_raw,
    NULL AS usd_amount,
    CASE
      WHEN dst = '\x0000000000000000000000000000000000000000' THEN '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
    ELSE
    dst
  END
    AS token_a_address,
    CASE
      WHEN src = '\x0000000000000000000000000000000000000000' THEN '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
    ELSE
    src
  END
    AS token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash AS tx_hash,
    NULL AS trace_address,
    log_index AS evt_index
  FROM
    blockchain-etl.ethereum_mooniswap.Mooniswap_event_Swapped ) dexs