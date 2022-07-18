SELECT  e.block_timestamp                                                                                                AS block_time,
        'Biswap'                                                                                                         AS project,
        '1'                                                                                                              AS version,
        'DEX'                                                                                                            AS category,
        309                                                                                                              AS protocol_id,
        e.to                                                                                                             AS trader_a,
        NULL                                                                                                             AS trader_b,
        CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, p.token0, p.token1).token_a = p.token0 THEN CAST(amount0Out as FLOAT64) ELSE CAST(amount1Out as FLOAT64) END AS token_a_amount_raw,
        CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, p.token0, p.token1).token_a = p.token0 THEN CAST(amount1In as FLOAT64) ELSE CAST(amount0In as FLOAT64) END AS token_b_amount_raw,
        NULL                                                                                                             AS usd_amount,
        `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, p.token0, p.token1).token_a AS token_a_address,
        `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, p.token0, p.token1).token_b AS token_b_address,
        e.contract_address                                                                                               AS exchange_contract_address,
        e.transaction_hash                                                                                               AS tx_hash,
        NULL                                                                                                             AS trace_address,
        e.log_index                                                                                                      AS evt_index
FROM `footprint-etl.bsc_biswap.BiswapPair_event_Swap` e
INNER JOIN `footprint-etl.bsc_biswap.BiswapFactory_event_PairCreated` p
ON p.pair = e.contract_address
WHERE DATE(e.block_timestamp) {match_date_filter}
