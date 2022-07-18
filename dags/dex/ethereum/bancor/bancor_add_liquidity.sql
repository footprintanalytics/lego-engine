
-- add_liquidity
SELECT
  'Bancor Network' AS project,
  a._provider AS liquidity_provider,
  a.version AS version,
  64 As protocol_id,
  t.symbol AS token_symbol,
  SAFE_DIVIDE(CAST(a._amount AS FLOAT64), POW(10, t.decimals)) as token_amount,
  CAST(null AS FLOAT64) AS usd_value_of_token,
  CAST(a._amount AS FLOAT64) AS token_amount_raw,
  'AMM' AS type,
  a._reserveToken as token_address,
  a.contract_address AS exchange_address,
  a.transaction_hash AS tx_hash,
  DATETIME(a.block_timestamp) AS block_time,
  tt.from_address AS tx_from
FROM
  (select
    *,
    '1' as version
    from
    blockchain-etl.ethereum_bancor.LiquidityPoolV1Converter_event_LiquidityAdded
    UNION ALL
    select
    *,
    '2' as version
    from
    footprint-etl.ethereum_bancor.LiquidityPoolV2Converter_event_LiquidityAdded
  ) a
  LEFT JOIN xed-project-237404.footprint_etl.erc20_tokens t ON Lower(t.contract_address) = a._reserveToken
  LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` tt ON tt.hash = a.transaction_hash AND DATE(tt.block_timestamp) {match_date_filter}
  where DATE(a.block_timestamp) {match_date_filter}
