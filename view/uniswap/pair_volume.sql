SELECT
  DATE(block_timestamp) as date_time,
  contract_address as pair,
  IEEE_DIVIDE(SUM(CAST(amount0In AS BIGNUMERIC)) + SUM(CAST(amount0Out AS BIGNUMERIC)),POW(10,18)) AS volume_A,
  IEEE_DIVIDE(SUM(CAST(amount1In AS BIGNUMERIC)) + SUM(CAST(amount1Out AS BIGNUMERIC)),POW(10,18)) AS volume_B
  FROM
  `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Swap`
  WHERE DATE(block_timestamp) > "2021-06-20"
  GROUP BY DATE(block_timestamp), contract_address
