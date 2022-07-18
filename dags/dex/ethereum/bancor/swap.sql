SELECT
  dexs.block_time,
  project,
  version,
  category,
  protocol_id,
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
    -- Bancor Network
  SELECT
    block_time,
    'Bancor Network' AS project,
    version,
    64 as protocol_id,
    'DEX' AS category,
    trader AS trader_a,
    NULL AS trader_b,
    target_token_amount_raw AS token_a_amount_raw,
    source_token_amount_raw AS token_b_amount_raw,
    NULL AS usd_amount,
    CASE
      WHEN target_token_address = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE
    target_token_address
  END
    AS token_a_address,
    CASE
      WHEN source_token_address = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    ELSE
    source_token_address
  END
    AS token_b_address,
    contract_address AS exchange_contract_address,
    tx_hash,
    NULL AS trace_address,
    evt_index
  FROM (
    SELECT
      _smartToken AS smart_token_address,
      _fromToken AS source_token_address,
      _toToken AS target_token_address,
      _fromAmount AS source_token_amount_raw,
      _toAmount AS target_token_amount_raw,
      _trader AS trader,
      version,
      contract_address,
      transaction_hash AS tx_hash,
      log_index AS evt_index,
      block_timestamp AS block_time
    FROM (
      SELECT
        *,
        '6' as version
      FROM
        footprint-etl.ethereum_bancor.BancorNetwork_v6_event_Conversion
      UNION ALL
      SELECT
        *,
        '7' as version
      FROM
        footprint-etl.ethereum_bancor.BancorNetwork_v7_event_Conversion
      UNION ALL
      SELECT
        *,
        '8' as version
      FROM
        footprint-etl.ethereum_bancor.BancorNetwork_v8_event_Conversion
      UNION ALL
      SELECT
        *,
        '9' as version
      FROM
        footprint-etl.ethereum_bancor.BancorNetwork_v9_event_Conversion
      UNION ALL
      SELECT
        *,
        '10' as version
      FROM
        footprint-etl.ethereum_bancor.BancorNetwork_v10_event_Conversion ) ) ) dexs