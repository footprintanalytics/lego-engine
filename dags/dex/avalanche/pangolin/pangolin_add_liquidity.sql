SELECT
    'Pangolin' as project,
    tx.from_address AS liquidity_provider,
    '1' as version,
    159 as protocol_id,
    t.symbol AS token_symbol,
    CAST(event.amount0 as float64) / POW(10, t.decimals) AS token_amount,
    (CAST (NULL AS float64)) AS usd_value_of_token,
    CAST(event.amount0 as float64) AS token_amount_raw,
    'AMM' AS type,
    token0 AS token_address,
    event.contract_address AS exchange_address,
    event.transaction_hash AS tx_hash,
    DATETIME(event.block_timestamp) AS block_time,
    tx.from_address AS tx_from
FROM
  `footprint-etl.avalanche_pangolin.PangolinLiquidity_event_Mint` event
LEFT JOIN
  `footprint-etl.avalanche_pangolin.PangolinFactory_event_PairCreated` pair_created
ON
  lower(event.contract_address) = lower(pair_created.pair)
LEFT JOIN `xed-project-237404.footprint_etl.avalanche_erc20_tokens` t ON t.contract_address = pair_created.token0
LEFT JOIN
  `footprint-blockchain-etl.crypto_avalanche.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}

union all

SELECT
    'Pangolin' as project,
    tx.from_address AS liquidity_provider,
    '1' as version,
    159 as protocol_id,
    t.symbol AS token_symbol,
    CAST(event.amount1 as float64) / POW(10, t.decimals) AS token_amount,
    (CAST (NULL AS float64)) AS usd_value_of_token,
    CAST(event.amount1 as float64) AS token_amount_raw,
    'AMM' AS type,
    token1 AS token_address,
    event.contract_address AS exchange_address,
    event.transaction_hash AS tx_hash,
    DATETIME(event.block_timestamp) AS block_time,
    tx.from_address AS tx_from
FROM
  `footprint-etl.avalanche_pangolin.PangolinLiquidity_event_Mint` event
LEFT JOIN
  `footprint-etl.avalanche_pangolin.PangolinFactory_event_PairCreated` pair_created
ON
  lower(event.contract_address) = lower(pair_created.pair)
LEFT JOIN `xed-project-237404.footprint_etl.avalanche_erc20_tokens` t ON t.contract_address = pair_created.token1
LEFT JOIN
  `footprint-blockchain-etl.crypto_avalanche.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}


