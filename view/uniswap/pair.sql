SELECT
'uniswap_v2' as name,
pair,
token0 as token_A,
token1 as token_B,
block_timestamp,
block_number
FROM
`blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated`
