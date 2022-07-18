        SELECT
            t.block_timestamp AS block_time,
            'Shibaswap' AS project,
            '1' AS version,
            'DEX' AS category,
            333 as protocol_id,
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
            `footprint-etl.ethereum_shibaswap.UniswapV2Pair_event_Swap` t
        INNER JOIN `footprint-etl.ethereum_shibaswap.UniswapV2Factory_event_PairCreated` f ON f.pair = t.contract_address
        WHERE DATE(t.block_timestamp) {match_date_filter}