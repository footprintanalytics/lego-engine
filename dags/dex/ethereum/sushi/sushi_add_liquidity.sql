-- add_liquidity

-- v1
-- token0

    SELECT
      'Sushiswap' AS project,
      event.sender AS liquidity_provider,
      '1' as version,
      16 as protocol_id,
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
      `blockchain-etl.ethereum_sushiswap.UniswapV2Pair_event_Mint` event
    LEFT JOIN
      `blockchain-etl.ethereum_sushiswap.UniswapV2Factory_event_PairCreated` pair_created
    ON
      (event.contract_address = pair_created.pair)
    LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = pair_created.token0
    LEFT JOIN
      `bigquery-public-data.crypto_ethereum.transactions` tx
    ON
      tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

    WHERE DATE(event.block_timestamp) {match_date_filter}

union all
    -- token1
    SELECT
      'Sushiswap' AS project,
      event.sender AS liquidity_provider,
       '1' as version,
      16 as protocol_id,
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
      `blockchain-etl.ethereum_sushiswap.UniswapV2Pair_event_Mint` event
    LEFT JOIN
      `blockchain-etl.ethereum_sushiswap.UniswapV2Factory_event_PairCreated` pair_created
    ON
      (event.contract_address = pair_created.pair)
      LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = pair_created.token1
    LEFT JOIN
      `bigquery-public-data.crypto_ethereum.transactions` tx
    ON
      tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

    WHERE DATE(event.block_timestamp) {match_date_filter}
