-- add_liquidity
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
    -- v1
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
              footprint-etl.bsc_ellipsis.pBTC_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.fUSDT_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.btcEPS_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.anyBTC_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.UST_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.USDN_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.TUSD_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.3EPS_swap_event_AddLiquidity
            UNION ALL
            SELECT
              *
            FROM
              footprint-etl.bsc_ellipsis.DAI_swap_event_AddLiquidity ) b
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
    DATE(a.block_timestamp) {match_date_filter} )