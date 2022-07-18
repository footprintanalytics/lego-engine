SELECT
    'Trader Joe' as project,
    '1' as version,
     532 as protocol_id,
    'DEX' as category,
    a.block_number,
    a.block_timestamp as block_time,
    a.transaction_hash as tx_hash,
    a.log_index as evt_index,
    a.contract_address,
    a.to as trader_a,
    NULL AS trader_b,
    CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(a.amount0In, a.amount0Out, a.amount1In, a.amount1Out, b.token0, b.token1).token_a = b.token0 THEN CAST(a.amount0Out as FLOAT64) ELSE CAST(a.amount1Out as FLOAT64) END AS token_a_amount_raw,
    CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(a.amount0In, a.amount0Out, a.amount1In, a.amount1Out, b.token0, b.token1).token_a = b.token0 THEN CAST(a.amount1In as FLOAT64) ELSE CAST(a.amount0In as FLOAT64) END AS token_b_amount_raw,
    NULL AS usd_amount,
    `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(a.amount0In, a.amount0Out, a.amount1In, a.amount1Out, b.token0, b.token1).token_a AS token_a_address, -- pool out
    `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(a.amount0In, a.amount0Out, a.amount1In, a.amount1Out, b.token0, b.token1).token_b AS token_b_address, -- pool in
    a.contract_address as exchange_contract_address,
    NULL AS trace_address
from `footprint-etl.avalanche_traderjoe.JoePair_event_Swap` a
left join `footprint-etl.avalanche_traderjoe.JoeFactory_event_PairCreated` b
on
    lower(a.contract_address) = lower(b.pair)
where Date(a.block_timestamp) {match_date_filter}
        