
            select
                DATETIME(dexs.block_time) AS block_time,
                tokena.symbol AS token_a_symbol,
                tokenb.symbol AS token_b_symbol,
                SAFE_DIVIDE(CAST(token_a_amount_raw AS FLOAT64), POW(10, tokena.decimals)) AS token_a_amount,
                SAFE_DIVIDE(CAST(token_b_amount_raw AS FLOAT64), POW(10, tokenb.decimals)) AS token_b_amount,
                project,
                version,
                category,
                protocol_id,
                tx.from_address AS trader_a,
                tx.to_address AS trader_b,
                CAST(token_a_amount_raw AS FLOAT64) AS token_a_amount_raw,
                CAST(token_b_amount_raw AS FLOAT64) AS token_b_amount_raw,
                token_a_address,
                token_b_address,
                exchange_contract_address,
                tx_hash,
                tx.from_address AS tx_from,
                tx.to_address AS tx_to,
                CAST(trace_address AS STRING) AS trace_address
            from (
                -- Sushiswap
SELECT
    t.block_timestamp AS block_time,
    'Sushiswap' AS project,
    '1' AS version,
    'DEX' AS category,
    291 as protocol_id,
    t.to AS trader_a,
    NULL AS trader_b,
    CASE WHEN CAST(amount0Out as FLOAT64) = 0 THEN CAST(amount1Out as FLOAT64) ELSE CAST(amount0Out as FLOAT64) END AS token_a_amount_raw,
    CASE WHEN CAST(amount0In as FLOAT64) = 0 THEN CAST(amount1In as FLOAT64) ELSE CAST(amount0In as FLOAT64) END AS token_b_amount_raw,
    NULL AS usd_amount,
    CASE WHEN CAST(amount0Out as FLOAT64) = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
    CASE WHEN CAST(amount0In as FLOAT64) = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
    t.contract_address exchange_contract_address,
    t.transaction_hash AS tx_hash,
    NULL AS trace_address,
    t.log_index as evt_index
FROM
    `footprint-etl.polygon_sushi.UniswapV2Pair_event_Swap` t
INNER JOIN `footprint-etl.polygon_sushi.UniswapV2Factory_event_PairCreated` f ON f.pair = t.contract_address
WHERE DATE(t.block_timestamp) = '2021-11-22'
            ) dexs
            inner join `public-data-finance.crypto_polygon.transactions` tx
            on dexs.tx_hash = tx.hash and DATE(tx.block_timestamp) = '2021-11-22'
            left join `xed-project-237404.footprint_etl.polygon_erc20_tokens` tokena
            on Lower(tokena.contract_address) = Lower(dexs.token_a_address)
            left join `xed-project-237404.footprint_etl.polygon_erc20_tokens` tokenb
            on Lower(tokenb.contract_address) = Lower(dexs.token_b_address)
            where tx.to_address is not null
            