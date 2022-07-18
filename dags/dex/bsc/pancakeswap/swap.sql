SELECT
    dexs.block_time,
    project,
    version,
    category,
    protocol_id,
    trader_b,
    token_a_amount_raw,
    token_b_amount_raw,
    NULL as usd_amount,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    tx_hash,
    trace_address,
    evt_index,
    row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id
FROM (
        SELECT
            t.block_timestamp AS block_time,
            100 AS protocol_id,
            'PancakeSwap' AS project,
            '2' AS version,
            'DEX' AS category,
            t.to AS trader_a,
            NULL AS trader_b,
            CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a = f.token0 THEN CAST(amount0Out as FLOAT64) ELSE CAST(amount1Out as FLOAT64) END AS token_a_amount_raw,
            CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a = f.token0 THEN CAST(amount1In as FLOAT64) ELSE CAST(amount0In as FLOAT64) END AS token_b_amount_raw,
            NULL AS usd_amount,
            `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a AS token_a_address,
            `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_b AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index AS evt_index
        FROM
            `footprint-etl.bsc_pancakeswap.PancakePair_event_Swap` t
        INNER JOIN `footprint-etl.bsc_pancakeswap.UniswapV2Pair_event_PairCreated` f ON f.pair = t.contract_address
        WHERE DATE(t.block_timestamp) {match_date_filter}
) dexs


    