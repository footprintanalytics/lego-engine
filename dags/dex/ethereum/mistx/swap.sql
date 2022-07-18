SELECT
        dexs.block_time,
        project,
        version,
        category,
        669 as protocol_id,
        token_a_amount_raw,
        token_b_amount_raw,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        trace_address,
        log_index
    FROM (

        -- mistX router for Sushiswap
        SELECT
            t.block_timestamp AS block_time,
            'mistX' AS project,
            '1' AS version,
            'DEX' AS category,
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
            t.log_index
        FROM
            `footprint-etl.ethereum_sushi.Pair_event_Swap` t
        INNER JOIN `footprint-etl.ethereum_sushi.Factory_event_PairCreated` f ON f.pair = t.contract_address

        UNION ALL

        -- mistX router for Uniswap v2
        SELECT
            t.block_timestamp AS block_time,
            'mistX' AS project,
            '1' AS version,
            'DEX' AS category,
            t.to AS trader_a,
            NULL AS trader_b,
            CASE WHEN CAST(amount0Out as FLOAT64) = 0 THEN CAST(amount1Out as FLOAT64) ELSE CAST(amount0Out as FLOAT64) END AS token_a_amount_raw,
            CASE WHEN CAST(amount0In as FLOAT64) = 0 THEN CAST(amount1In as FLOAT64) ELSE CAST(amount0In as FLOAT64) END AS token_b_amount_raw,
            NULL AS usd_amount,
            CASE WHEN CAST(amount0Out as FLOAT64) = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
            CASE WHEN CAST(amount0In as FLOAT64) = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index
        FROM
            `footprint-etl.ethereum_uniswap.UniswapV2Pair_event_Swap` t
        INNER JOIN `footprint-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated` f ON f.pair = t.contract_address
        WHERE t.contract_address NOT IN (
            '0xed9c854cb02de75ce4c9bba992828d6cb7fd5c71', -- remove WETH-UBOMB wash trading pair
            '0x854373387e41371ac6e307a1f29603c6fa10d872' ) -- remove FEG/ETH token pair
    ) dexs