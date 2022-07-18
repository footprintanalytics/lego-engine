-- Spookyswap
SELECT
    t.block_timestamp AS block_time,
    'Spookyswap' AS project,
    '1' AS version,
    'DEX' AS category,
    214 as protocol_id,
    t.to AS trader_a,
    NULL AS trader_b,
    CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a = f.token0 THEN CAST(amount0Out as FLOAT64) ELSE CAST(amount1Out as FLOAT64) END AS token_a_amount_raw,
    CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a = f.token0 THEN CAST(amount1In as FLOAT64) ELSE CAST(amount0In as FLOAT64) END AS token_b_amount_raw,
    NULL AS usd_amount,
    `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a AS token_a_address,
    `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_b AS token_b_address,
    t.contract_address exchange_contract_address,
    t.transaction_hash AS tx_hash,
    NULL AS trace_address,
    t.log_index as evt_index
FROM
    `footprint-etl.fantom_spookyswap.Pair_event_Swap` t
INNER JOIN `footprint-etl.fantom_spookyswap.Factory_event_PairCreated` f ON f.pair = t.contract_address
WHERE DATE(t.block_timestamp) {match_date_filter}