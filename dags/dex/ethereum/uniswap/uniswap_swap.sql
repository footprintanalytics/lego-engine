        -- Uniswap v1 TokenPurchase
--        SELECT
--            t.block_timestamp AS block_time,
--            'Uniswap' AS project,
--            '1' AS version,
--            'DEX' AS category,
--            400 as protocol_id,
--            buyer AS trader_a,
--            NULL AS trader_b,
--            CAST(tokens_bought as FLOAT64) AS token_a_amount_raw,
--            CAST(eth_sold as FLOAT64) AS token_b_amount_raw,
--            NULL AS usd_amount,
--            f.token AS token_a_address,  -- pool out
--            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address, --Using WETH for easier joining with USD price table   pool in
--            t.contract_address exchange_contract_address,
--            t.transaction_hash AS tx_hash,
--            NULL AS trace_address,
--            t.log_index AS evt_index
--        FROM
--            `blockchain-etl.ethereum_uniswap.Uniswap_event_TokenPurchase` t
--        INNER JOIN `blockchain-etl.ethereum_uniswap.Vyper_contract_event_NewExchange` f ON f.exchange = t.contract_address
--        WHERE DATE(t.block_timestamp) {match_date_filter}
--        UNION ALL
--
--        -- Uniswap v1 EthPurchase
--        SELECT
--            t.block_timestamp AS block_time,
--            'Uniswap' AS project,
--            '1' AS version,
--            'DEX' AS category,
--            400 as protocol_id,
--            buyer AS trader_a,
--            NULL AS trader_b,
--            CAST(eth_bought as FLOAT64) AS token_a_amount_raw,
--            CAST(tokens_sold as FLOAT64) AS token_b_amount_raw,
--            NULL AS usd_amount,  -- pool out
--            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' token_a_address, --Using WETH for easier joining with USD price table  pool in
--            f.token AS token_b_address,
--            t.contract_address exchange_contract_address,
--            t.transaction_hash AS tx_hash,
--            NULL AS trace_address,
--            t.log_index AS evt_index
--        FROM
--            `blockchain-etl.ethereum_uniswap.Uniswap_event_EthPurchase` t
--        INNER JOIN `blockchain-etl.ethereum_uniswap.Vyper_contract_event_NewExchange` f ON f.exchange = t.contract_address
--        WHERE DATE(t.block_timestamp) {match_date_filter}
--
--        UNION ALL
        -- Uniswap v2
        SELECT
            t.block_timestamp AS block_time,
            'Uniswap' AS project,
            '2' AS version,
            'DEX' AS category,
            1 as protocol_id,
            t.to AS trader_a,
            NULL AS trader_b,
            CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a = f.token0 THEN CAST(amount0Out as FLOAT64) ELSE CAST(amount1Out as FLOAT64) END AS token_a_amount_raw,
            CASE WHEN `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a = f.token0 THEN CAST(amount1In as FLOAT64) ELSE CAST(amount0In as FLOAT64) END AS token_b_amount_raw,
            NULL AS usd_amount,
            `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_a AS token_a_address, -- pool out
            `xed-project-237404.footprint_etl.GetUniswapV2SwapToken`(amount0In, amount0Out, amount1In, amount1Out, f.token0, f.token1).token_b AS token_b_address, -- pool in
            t.contract_address AS exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index AS evt_index
        FROM
            `blockchain-etl.ethereum_uniswap.UniswapV2Pair_event_Swap` t
        INNER JOIN `blockchain-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated` f ON f.pair = t.contract_address
        WHERE t.contract_address NOT IN (
            '0xed9c854cb02de75ce4c9bba992828d6cb7fd5c71', -- remove WETH-UBOMB wash trading pair
            '0x854373387e41371ac6e307a1f29603c6fa10d872' ) -- remove FEG/ETH token pair
        AND DATE(t.block_timestamp) {match_date_filter}

        UNION ALL
        --Uniswap v3
        SELECT
            t.block_timestamp AS block_time,
            'Uniswap' AS project,
            '3' AS version,
            'DEX' AS category,
            217 as protocol_id,
            t.recipient AS trader_a,
            NULL AS trader_b,
            CASE WHEN CAST(amount0 as FLOAT64) < 0 THEN abs(CAST(amount0 as FLOAT64)) ELSE abs(CAST(amount1 as FLOAT64)) END AS token_a_amount_raw,
            CASE WHEN CAST(amount0 as FLOAT64) < 0 THEN abs(CAST(amount1 as FLOAT64)) ELSE abs(CAST(amount0 as FLOAT64)) END AS token_b_amount_raw,
            NULL AS usd_amount,
            CASE WHEN CAST(amount0 as FLOAT64) < 0 THEN f.token0 ELSE f.token1 END AS token_a_address, -- pool out
            CASE WHEN CAST(amount0 as FLOAT64) < 0 THEN f.token1 ELSE f.token0 END AS token_b_address, -- pool in
            t.contract_address as exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index AS evt_index
        FROM
            `blockchain-etl.ethereum_uniswap.UniswapV3Pool_event_Swap` t
        INNER JOIN `blockchain-etl.ethereum_uniswap.UniswapV3Factory_event_PoolCreated` f ON f.pool = t.contract_address
        WHERE DATE(t.block_timestamp) {match_date_filter}