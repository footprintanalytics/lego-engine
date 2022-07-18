-- add_liquidity

-- v1
-- token0
SELECT
  '{project}' AS project,
  event.sender AS liquidity_provider,
  version,
  protocol_id,
  CAST(NULL as float64) AS eth_amount,
  CAST(NULL AS float64) AS usd_value_of_eth,
  t.symbol AS token_symbol,
  CAST(event.amount0 as float64) / POW(10, t.decimals) AS token_amount,
  (CAST (NULL AS float64)) AS usd_value_of_token,
  CAST(event.amount0 as float64) AS token_amount_raw,
  'add' AS type,
  token0 AS token_address,
  event.contract_address AS exchange_address,
  event.transaction_hash AS tx_hash,
  DATETIME(event.block_timestamp) AS block_time,
  tx.from_address AS tx_from
FROM
  `{mint}` event
LEFT JOIN
  `{pair_created}` pair_created
ON
  (event.contract_address = pair_created.pair)
LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = event.contract_address
LEFT JOIN
  `bigquery-public-data.crypto_ethereum.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}

union all
-- token1
SELECT
  '{project}' AS project,
  event.sender AS liquidity_provider,
  CAST(NULL as float64) AS eth_amount,
  CAST(NULL AS float64) AS usd_value_of_eth,
  t.symbol AS token_symbol,
  CAST(event.amount1 as float64) / POW(10, t.decimals) AS token_amount,
  (CAST (NULL AS float64)) AS usd_value_of_token,
  CAST(event.amount1 as float64) AS token_amount_raw,
  'add' AS type,
  token1 AS token_address,
  event.contract_address AS exchange_address,
  event.transaction_hash AS tx_hash,
  DATETIME(event.block_timestamp) AS block_time,
  tx.from_address AS tx_from
FROM
  `{mint}` event
LEFT JOIN
  `{pair_created}` pair_created
ON
  (event.contract_address = pair_created.pair)
  LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = event.contract_address
LEFT JOIN
  `bigquery-public-data.crypto_ethereum.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}

-- remove_liquidity
union all

-- v1
-- token0
SELECT
  '{project}' AS project,
  event.sender AS liquidity_provider,
  CAST(NULL as float64) AS eth_amount,
  CAST(NULL AS float64) AS usd_value_of_eth,
  t.symbol AS token_symbol,
  CAST(event.amount0 as float64) / POW(10, t.decimals) AS token_amount,
  (CAST (NULL AS float64)) AS usd_value_of_token,
  CAST(event.amount0 as float64) AS token_amount_raw,
  'remove' AS type,
  token0 AS token_address,
  event.contract_address AS exchange_address,
  event.transaction_hash AS tx_hash,
  DATETIME(event.block_timestamp) AS block_time,
  tx.from_address AS tx_from
FROM
  `{burn}` event
LEFT JOIN
  `{pair_created}` pair_created
ON
  (event.contract_address = pair_created.pair)
  LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = event.contract_address
LEFT JOIN
  `bigquery-public-data.crypto_ethereum.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}

union all

-- token1
SELECT
  '{project}' AS project,
  event.sender AS liquidity_provider,
  CAST(NULL as float64) AS eth_amount,
  CAST(NULL AS float64) AS usd_value_of_eth,
  t.symbol AS token_symbol,
  CAST(event.amount1 as float64) / POW(10, t.decimals) AS token_amount,
  (CAST (NULL AS float64)) AS usd_value_of_token,
  CAST(event.amount1 as float64) AS token_amount_raw,
  'remove' AS type,
  token1 AS token_address,
  event.contract_address AS exchange_address,
  event.transaction_hash AS tx_hash,
  DATETIME(event.block_timestamp) AS block_time,
  tx.from_address AS tx_from
FROM
  `{burn}` event
LEFT JOIN
  `{pair_created}` pair_created
ON
  (event.contract_address = pair_created.pair)
  LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = event.contract_address
LEFT JOIN
  `bigquery-public-data.crypto_ethereum.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}