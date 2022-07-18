SELECT
  block_time,
  project,
  344 AS protocol_id,
  version,
  category,
  trader_b,
  token_a_amount_raw,
  token_b_amount_raw,
  NULL as usd_amount,
  token_a_address,
  token_b_address,
  exchange_contract_address,
  tx_hash,
  trace_address,
  evt_index,
  row_number() OVER (
    PARTITION BY project,
    tx_hash,
    evt_index,
    trace_address
    ORDER BY
      version,
      category
  ) AS trade_id
FROM
  (
    -- Tokenlon V4
    SELECT
      block_timestamp AS block_time,
      'Tokenlon' AS project,
      '4' AS version,
      'Aggregator' AS category,
      takerAddress AS trader_a,
      makerAddress AS trader_b,
      takerAssetFilledAmount AS token_a_amount_raw,
      makerAssetFilledAmount AS token_b_amount_raw,
      NULL AS usd_amount,
      concat('0x', SUBSTR(takerAssetData, 35, 40)) AS token_a_address,
      concat('0x', SUBSTR(makerAssetData, 35, 40)) AS token_b_address,
      contract_address AS exchange_contract_address,
      transaction_hash AS tx_hash,
      NULL AS trace_address,
      log_index AS evt_index
    FROM
      `footprint-etl.ethereum_zeroex.Exchange2_1_event_Fill` t
    WHERE
      feeRecipientAddress IN ('0xb9e29984fe50602e7a619662ebed4f90d93824c7')
    UNION
    ALL -- Tokenlon V5
    SELECT
      block_timestamp AS block_time,
      'Tokenlon' AS project,
      '5' AS version,
      'Aggregator' AS category,
      takerAddress AS trader_a,
      makerAddress AS trader_b,
      takerAssetFilledAmount AS token_a_amount_raw,
      makerAssetFilledAmount AS token_b_amount_raw,
      NULL AS usd_amount,
      concat('0x', SUBSTR(takerAssetData, 35, 40)) AS token_a_address,
      concat('0x', SUBSTR(makerAssetData, 35, 40)) AS token_b_address,
      contract_address AS exchange_contract_address,
      transaction_hash AS tx_hash,
      NULL AS trace_address,
      log_index AS evt_index
    FROM
      `footprint-etl.ethereum_zeroex.Exchange2_1_event_Fill`
    WHERE
      takerAddress IN ('0x8d90113a1e286a5ab3e496fbd1853f265e5913c6')
    UNION
    ALL -- Tokenlon V5 AMMWrapper
    SELECT
      block_timestamp AS block_time,
      'Tokenlon' AS project,
      '5' AS version,
      'Aggregator' AS category,
      userAddr AS trader_a,
      makerAddr AS trader_b,
      takerAssetAmount AS token_a_amount_raw,
      makerAssetAmount AS token_b_amount_raw,
      NULL AS usd_amount,
      CASE
        WHEN takerAssetAddr IN ('0x0000000000000000000000000000000000000000') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE takerAssetAddr
      END AS token_a_address,
      CASE
        WHEN makerAssetAddr IN ('0x0000000000000000000000000000000000000000') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE makerAssetAddr
      END AS token_b_address,
      contract_address AS exchange_contract_address,
      transaction_hash AS tx_hash,
      NULL AS trace_address,
      log_index AS evt_index
    FROM
      `footprint-etl.ethereum_tokenlon.AMMWrapper_event_Swapped`
    UNION
    ALL -- Tokenlon V5 AMMWrapperWithPath swapped0 event
    SELECT
      block_timestamp AS block_time,
      'Tokenlon' AS project,
      '5' AS version,
      'Aggregator' AS category,
      userAddr AS trader_a,
      makerAddr AS trader_b,
      takerAssetAmount AS token_a_amount_raw,
      makerAssetAmount AS token_b_amount_raw,
      NULL AS usd_amount,
      CASE
        WHEN takerAssetAddr IN ('0x0000000000000000000000000000000000000000') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE takerAssetAddr
      END AS token_a_address,
      CASE
        WHEN makerAssetAddr IN ('0x0000000000000000000000000000000000000000') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE makerAssetAddr
      END AS token_b_address,
      contract_address AS exchange_contract_address,
      transaction_hash AS tx_hash,
      NULL AS trace_address,
      log_index AS evt_index
    FROM
      `footprint-etl.ethereum_tokenlon.AMMWrapperWithPath_event_Swapped`
    UNION
    ALL -- Tokenlon V5 AMMWrapperWithPath swapped event
    SELECT
      block_timestamp AS block_time,
      'Tokenlon' AS project,
      '5' AS version,
      'Aggregator' AS category,
      userAddr AS trader_a,
      makerAddr AS trader_b,
      takerAssetAmount AS token_a_amount_raw,
      makerAssetAmount AS token_b_amount_raw,
      NULL AS usd_amount,
      CASE
        WHEN takerAssetAddr = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE takerAssetAddr
      END AS token_a_address,
      CASE
        WHEN makerAssetAddr = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE makerAssetAddr
      END AS token_b_address,
      contract_address AS exchange_contract_address,
      transaction_hash AS tx_hash,
      NULL AS trace_address,
      log_index AS evt_index
    FROM
      `footprint-etl.ethereum_tokenlon.AMMWrapperWithPath_event_Swapped`
  )
