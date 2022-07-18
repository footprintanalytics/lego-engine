--v1
-- add_liquidity
--SELECT
--    'Uniswap' AS project,
--    a.provider AS liquidity_provider,
--    '1' as version,
--    400 as protocol_id,
--    t.symbol AS token_symbol,
--    CAST(a.token_amount as float64) / POW(10, t.decimals) AS token_amount,
--    (CAST (NULL AS float64)) AS usd_value_of_token,
--    CAST(a.token_amount as float64) AS token_amount_raw,
--    'AMM' AS type,
--    e.token AS token_address,
--    a.contract_address AS exchange_address,
--    a.transaction_hash AS tx_hash,
--    DATETIME(a.block_timestamp) AS block_time,
--    tt.from_address AS tx_from
--FROM
--    `blockchain-etl.ethereum_uniswap.Uniswap_Dai_event_AddLiquidity` a
--    LEFT JOIN `blockchain-etl.ethereum_uniswap.Vyper_contract_event_NewExchange` e ON e.exchange = a.contract_address
--    LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = e.token
--    LEFT JOIN (
--    select transaction_hash, from_address from (
--        select token.transaction_hash, token.from_address, token.to_address,
--        ROW_NUMBER() OVER(PARTITION BY transaction_hash ORDER BY log_index) AS rn
--        FROM `bigquery-public-data.crypto_ethereum.token_transfers` token
--        where Date(token.block_timestamp) {match_date_filter}
--    ) where rn = 1
--  ) as tt on tt.transaction_hash = a.transaction_hash
--WHERE DATE(a.block_timestamp) {match_date_filter}

-- v2
-- add_liquidity
--union all

-- token0
SELECT
  'Uniswap' AS project,
  event.sender AS liquidity_provider,
  '2' as version,
  1 as protocol_id,
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
  `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Mint` event
LEFT JOIN
  `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated` pair_created
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
  'Uniswap' AS project,
  event.sender AS liquidity_provider,
  '2' as version,
  1 as protocol_id,
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
  `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Mint` event
LEFT JOIN
  `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated` pair_created
ON
  (event.contract_address = pair_created.pair)
  LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t ON t.contract_address = pair_created.token1
LEFT JOIN
  `bigquery-public-data.crypto_ethereum.transactions` tx
ON
  tx.hash = event.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}

WHERE DATE(event.block_timestamp) {match_date_filter}

--v3
-- add_liquidity
-- token0
union all
SELECT
    'Uniswap' AS project,
    b.to AS liquidity_provider,
    '3' as version,
    217 as protocol_id,
    e.symbol AS token_symbol,
    CAST(a.amount0 as float64) / POW(10, e.decimals) AS token_amount,
    (CAST (NULL AS float64)) AS usd_value_of_token,
    CAST(a.amount0 as float64) AS token_amount_raw,
    'AMM' AS type,
    d.token0 AS token_address,
    d.pool AS exchange_address,
    a.transaction_hash AS tx_hash,
    DATETIME(a.block_timestamp) AS block_time,
    b.to AS tx_from
FROM
    `footprint-etl.ethereum_uniswap.UniswapV3PositionsNFT_event_IncreaseLiquidity` a
left join (
    select * from `footprint-etl.ethereum_uniswap.UniswapV3PositionsNFT_event_Transfer`
    where `from` = '0x0000000000000000000000000000000000000000'
) b
on
    a.tokenId = b.tokenId
left join
    (select distinct contract_address, transaction_hash, amount0, amount1 from `footprint-etl.ethereum_uniswap.UniswapV3Pool_event_Mint` ) c
on
    lower(a.transaction_hash) = lower(c.transaction_hash) and a.amount0 = c.amount0 and a.amount1 = c.amount1
left join
    `footprint-etl.ethereum_uniswap.UniswapV3Factory_event_PoolCreated` d
on
    lower(d.pool) = lower(c.contract_address)
left join
    `xed-project-237404.footprint_etl.erc20_tokens` e
on
    lower(e.contract_address) = lower(d.token0)
WHERE DATE(a.block_timestamp) {match_date_filter}

union all
-- token 1
SELECT
    'Uniswap' AS project,
    b.to AS liquidity_provider,
    '3' as version,
    217 as protocol_id,
    e.symbol AS token_symbol,
    CAST(a.amount1 as float64) / POW(10, e.decimals) AS token_amount,
    (CAST (NULL AS float64)) AS usd_value_of_token,
    CAST(a.amount1 as float64) AS token_amount_raw,
    'AMM' AS type,
    d.token1 AS token_address,
    d.pool AS exchange_address,
    a.transaction_hash AS tx_hash,
    DATETIME(a.block_timestamp) AS block_time,
    b.to AS tx_from
FROM
    `footprint-etl.ethereum_uniswap.UniswapV3PositionsNFT_event_IncreaseLiquidity` a
left join (
    select * from `footprint-etl.ethereum_uniswap.UniswapV3PositionsNFT_event_Transfer`
    where `from` = '0x0000000000000000000000000000000000000000'
) b
on
    a.tokenId = b.tokenId
left join
    (select distinct contract_address, transaction_hash, amount0, amount1 from `footprint-etl.ethereum_uniswap.UniswapV3Pool_event_Mint` ) c
on
    lower(a.transaction_hash) = lower(c.transaction_hash) and a.amount0 = c.amount0 and a.amount1 = c.amount1
left join
    `footprint-etl.ethereum_uniswap.UniswapV3Factory_event_PoolCreated` d
on
    lower(d.pool) = lower(c.contract_address)
left join
    `xed-project-237404.footprint_etl.erc20_tokens` e
on
    lower(e.contract_address) = lower(d.token1)
WHERE DATE(a.block_timestamp) {match_date_filter}
