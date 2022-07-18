
-- add_liquidity

SELECT
    'Balancer' AS project,
    LOWER(liquidity_provider) AS liquidity_provider,
    version,
    12 As protocol_id,
    token_symbol,
    token_amount,
    CAST(null AS FLOAT64) AS usd_value_of_token,
    token_amount_raw,
    'AMM' AS type,
    LOWER(token_address) AS token_address,
    LOWER(exchange_address) AS exchange_address,
    tx_hash,
    block_time,
    LOWER(tx_from) AS tx_from
FROM (
-- v2
    SELECT
      a.liquidityProvider AS liquidity_provider,
      '2' as version,
      t.symbol AS token_symbol,
      SAFE_DIVIDE(CAST(token_amount AS FLOAT64), POW(10, t.decimals)) as token_amount,
      CAST(token_amount AS FLOAT64) AS token_amount_raw,
      token_address as token_address,
      a.contract_address AS exchange_address,
      a.transaction_hash AS tx_hash,
      DATETIME(a.block_timestamp) AS block_time,
      tx.from_address AS tx_from
    FROM
        (SELECT * FROM (
            SELECT
                block_timestamp,
                transaction_hash,
                liquidityProvider,
                contract_address,
                token_address,
                token_amount
            FROM (
              SELECT *
              FROM (
                SELECT
                  *,
                  SPLIT(b.tokens) AS tokensArray,
                  SPLIT(b.deltas) AS tokensAmountArray
                FROM
                  blockchain-etl.ethereum_balancer.V2_Vault_event_PoolBalanceChanged b
                WHERE
                  REGEXP_CONTAINS(b.deltas, r'-[1-9]\d*') = FALSE),
                UNNEST(tokensArray) AS token_address WITH OFFSET)
                JOIN
                UNNEST(tokensAmountArray) AS token_amount WITH OFFSET
            USING(OFFSET))) a
        LEFT JOIN xed-project-237404.footprint_etl.erc20_tokens t ON Lower(t.contract_address) = Lower(token_address)
        LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` tx ON tx.hash = a.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}
    where DATE(a.block_timestamp) {match_date_filter}
    UNION ALL
--     v1
    SELECT
      a.caller AS liquidity_provider,
      '1' as version,
      t.symbol AS token_symbol,
      SAFE_DIVIDE(CAST(a.tokenAmountIn AS FLOAT64), POW(10, t.decimals)) as token_amount,
      CAST(a.tokenAmountIn AS FLOAT64) AS token_amount_raw,
      a.tokenIn as token_address,
      a.contract_address AS exchange_address,
      a.transaction_hash AS tx_hash,
      DATETIME(a.block_timestamp) AS block_time,
      tx.from_address AS tx_from
    FROM
      blockchain-etl.ethereum_balancer.BPool_event_LOG_JOIN a
      LEFT JOIN xed-project-237404.footprint_etl.erc20_tokens t ON Lower(t.contract_address) = a.tokenIn
      LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` tx ON tx.hash = a.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}
      where DATE(a.block_timestamp) {match_date_filter}
)


