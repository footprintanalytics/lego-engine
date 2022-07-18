
SELECT
  'Ellipsis' AS project,
  LOWER(liquidity_provider) AS liquidity_provider,
  version,
  148 AS protocol_id,
  token_symbol,
  token_amount,
  CAST(NULL AS FLOAT64) AS usd_value_of_token,
  token_amount_raw,
  'AMM' AS type,
  LOWER(token_address) AS token_address,
  LOWER(exchange_address) AS exchange_address,
  tx_hash,
  block_time,
  LOWER(tx_from) AS tx_from
FROM (
----  remove liquidity
  SELECT
    a.provider AS liquidity_provider,
    '1' AS version,
    t.symbol AS token_symbol,
    SAFE_DIVIDE(CAST(token_amount AS FLOAT64),
      POW(10, t.decimals)) AS token_amount,
    CAST(token_amount AS FLOAT64) AS token_amount_raw,
    token_address AS token_address,
    a.contract_address AS exchange_address,
    a.transaction_hash AS tx_hash,
    DATETIME(a.block_timestamp) AS block_time,
    tx.from_address AS tx_from
  FROM (
    SELECT
      *
    FROM (
      SELECT
        block_timestamp,
        transaction_hash,
        provider,
        contract_address,
        token_address,
        token_amount
      FROM (
        SELECT
          *
        FROM (
          SELECT
            *,
            pools.stake_token AS tokensArray,
            SPLIT(b.token_amounts) AS tokensAmountArray
          FROM (
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.pBTC_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.fUSDT_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.btcEPS_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.anyBTC_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.UST_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.USDN_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.TUSD_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.3EPS_swap_event_RemoveLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.DAI_swap_event_RemoveLiquidity
              ) b
          LEFT JOIN
            xed-project-237404.footprint_etl.pool_infos pools
          ON
            LOWER(pools.pool_id) = b.contract_address ),
          UNNEST(tokensArray) AS token_address
        WITH
        OFFSET
          )
      JOIN
        UNNEST(tokensAmountArray) AS token_amount
      WITH
      OFFSET
      USING
        (
        OFFSET
          ))) a
  LEFT JOIN
    xed-project-237404.footprint_etl.bsc_erc20_tokens t
  ON
    LOWER(t.contract_address) = LOWER(token_address)
  LEFT JOIN
    `footprint-blockchain-etl.crypto_bsc.transactions` tx
  ON
    tx.hash = a.transaction_hash
    AND DATE(tx.block_timestamp) {match_date_filter}
  WHERE
    DATE(a.block_timestamp) {match_date_filter}
----  remove liquidity Imbalance
UNION ALL
  SELECT
    a.provider AS liquidity_provider,
    '1' AS version,
    t.symbol AS token_symbol,
    SAFE_DIVIDE(CAST(token_amount AS FLOAT64),
      POW(10, t.decimals)) AS token_amount,
    CAST(token_amount AS FLOAT64) AS token_amount_raw,
    token_address AS token_address,
    a.contract_address AS exchange_address,
    a.transaction_hash AS tx_hash,
    DATETIME(a.block_timestamp) AS block_time,
    tx.from_address AS tx_from
  FROM (
    SELECT
      *
    FROM (
      SELECT
        block_timestamp,
        transaction_hash,
        provider,
        contract_address,
        token_address,
        token_amount
      FROM (
        SELECT
          *
        FROM (
          SELECT
            *,
            pools.stake_token AS tokensArray,
            SPLIT(b.token_amounts) AS tokensAmountArray
          FROM (
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.pBTC_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.fUSDT_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.btcEPS_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.anyBTC_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.UST_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.USDN_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.TUSD_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.3EPS_swap_event_RemoveLiquidityImbalance
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.DAI_swap_event_RemoveLiquidityImbalance
              ) b
          LEFT JOIN
            xed-project-237404.footprint_etl.pool_infos pools
          ON
            LOWER(pools.pool_id) = b.contract_address ),
          UNNEST(tokensArray) AS token_address
        WITH
        OFFSET
          )
      JOIN
        UNNEST(tokensAmountArray) AS token_amount
      WITH
      OFFSET
      USING
        (
        OFFSET
          ))) a
  LEFT JOIN
    xed-project-237404.footprint_etl.bsc_erc20_tokens t
  ON
    LOWER(t.contract_address) = LOWER(token_address)
  LEFT JOIN
    `footprint-blockchain-etl.crypto_bsc.transactions` tx
  ON
    tx.HASH = a.transaction_hash
    AND DATE(tx.block_timestamp) {match_date_filter}
  WHERE
    DATE(a.block_timestamp) {match_date_filter}

-- remove liquidity one
UNION  ALL
SELECT
  provider AS liquidity_provider,
  '1' AS version,
  token_symbol,
  SAFE_DIVIDE(CAST(token_amount AS FLOAT64),
    POW(10, decimals)) AS token_amount,
  CAST(token_amount AS FLOAT64) AS token_amount_raw,
  token_address AS token_address,
  contract_address AS exchange_address,
  transaction_hash AS tx_hash,
  DATETIME(block_timestamp) AS block_time,
  from_address AS tx_from
FROM (
  SELECT
    rl.transaction_hash AS transaction_hash,
    rl.block_timestamp AS block_timestamp,
    tt.to_address AS provider,
    tt.value AS token_amount,
    tt.token_address AS token_address,
    rl.contract_address AS contract_address,
    tx.from_address AS from_address,
    t.decimals AS decimals,
    t.symbol AS token_symbol,
  FROM (
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.pBTC_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.fUSDT_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *,
      NULL AS token_supply
    FROM
      footprint-etl.bsc_ellipsis.btcEPS_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.anyBTC_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.UST_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.USDN_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.TUSD_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *,
      NULL AS token_supply
    FROM
      footprint-etl.bsc_ellipsis.3EPS_swap_event_RemoveLiquidityOne
    UNION ALL
    SELECT
      *
    FROM
      footprint-etl.bsc_ellipsis.DAI_swap_event_RemoveLiquidityOne ) rl
  LEFT JOIN
    `footprint-blockchain-etl.crypto_bsc.transactions` tx
  ON
    tx.hash = rl.transaction_hash
    AND DATE(tx.block_timestamp) {match_date_filter}
  LEFT JOIN
    footprint-blockchain-etl.crypto_bsc.token_transfers tt
  ON
    LOWER(tt.transaction_hash) = LOWER(rl.transaction_hash)
    AND tt.to_address = tx.from_address
    AND DATE(tt.block_timestamp) {match_date_filter}
  LEFT JOIN
    xed-project-237404.footprint_etl.bsc_erc20_tokens t
  ON
    LOWER(t.contract_address) = LOWER(tt.token_address)
  WHERE
    DATE(rl.block_timestamp) {match_date_filter})
WHERE provider IS NOT NULL
)