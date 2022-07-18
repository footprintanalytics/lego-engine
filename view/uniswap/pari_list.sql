SELECT
pair.*,
price.price_A,
price.price_B,
volume.date_time,
volume.volume_A,
volume.volume_B
FROM
(
  SELECT
  'uniswap_v2' as name,
  pair,
  token0 as token_A,
  token1 as token_B
  FROM
  `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated`
) pair
LEFT JOIN
(
  SELECT
  DATE(block_timestamp) as date_time,
  contract_address as pair,
  IEEE_DIVIDE(SUM(CAST(amount0In AS BIGNUMERIC)) + SUM(CAST(amount0Out AS BIGNUMERIC)),POW(10,18)) AS volume_A,
  IEEE_DIVIDE(SUM(CAST(amount1In AS BIGNUMERIC)) + SUM(CAST(amount1Out AS BIGNUMERIC)),POW(10,18)) AS volume_B
  FROM
  `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Swap`
  WHERE DATE(block_timestamp) = "2021-06-20"
  GROUP BY DATE(block_timestamp), contract_address
) volume
ON pair.pair = volume.pair
LEFT JOIN
(
  SELECT
  DATE(block_timestamp) as date_time,
  contract_address as pair,
  AVG(CAST(reserve1 AS BIGNUMERIC)/CAST(reserve0 AS BIGNUMERIC)) as price_A,
  AVG(CAST(reserve0 AS BIGNUMERIC)/CAST(reserve1 AS BIGNUMERIC)) as price_B,
  FROM `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Sync`
  WHERE DATE(block_timestamp) = "2021-06-20"
  GROUP BY DATE(block_timestamp), contract_address
) price
ON pair.pair = price.pair
WHERE volume.volume_A > 0
ORDER BY volume.pair, volume.date_time
