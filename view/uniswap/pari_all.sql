SELECT
block_timestamp,
block_number,
transaction_hash,
contract_address,
sender,
token0,
token1,
token0_symbol,
token1_symbol,
token0_decimals,
token1_decimals,
volume0/POW(10, token0_decimals) as volume0,
volume1/POW(10, token1_decimals) as volume1,
(volume0/POW(10, token0_decimals))/(volume1/POW(10, token1_decimals)) as price0,
(volume1/POW(10, token1_decimals))/(volume0/POW(10, token0_decimals)) as price1
FROM
(
    SELECT
    volume.*,
    pair.*,
    token0.symbol as token0_symbol,
    token1.symbol as token1_symbol,
    IFNULL(CAST(token0.decimals AS INT64), 18) as token0_decimals,
    IFNULL(CAST(token1.decimals AS INT64), 18) as token1_decimals
    FROM
    (
      SELECT
      *,
      CAST(amount0In AS BIGNUMERIC) + CAST(amount0Out AS BIGNUMERIC) AS volume0,
      CAST(amount1In AS BIGNUMERIC) + CAST(amount1Out AS BIGNUMERIC) AS volume1
      FROM
      `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Swap`
      WHERE DATE(block_timestamp) = "2021-07-11"
    ) volume
    LEFT JOIN
    (
      SELECT
      pair,
      token0,
      token1,
      FROM
      `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated`
    ) pair
    ON pair.pair = volume.contract_address
    LEFT JOIN
    `bigquery-public-data.crypto_ethereum.amended_tokens`  token0
    ON pair.token0 = token0.address
    LEFT JOIN
    `bigquery-public-data.crypto_ethereum.amended_tokens`  token1
    ON pair.token1 = token1.address
    WHERE volume1 > 0 AND volume0 > 0
)
ORDER BY block_number