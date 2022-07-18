SELECT
        dexs.block_time,
        'Paraswap' AS project,
        '1' AS version,
        'Aggregator' AS category,
        1036 as protocol_id,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        null AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address) AS trade_id

    FROM (

        -- AugustusSwapper_event_Swapped
        SELECT
            swaps.block_timestamp	 AS block_time,
            swaps.user AS trader_a,
            NULL AS trader_b,
            swaps.srcAmount AS token_a_amount_raw,
            swaps.receivedAmount AS token_b_amount_raw,
            swaps.srcToken AS token_a_address,
            swaps.destToken AS token_b_address,
            swaps.contract_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper_event_Swapped swaps
        UNION ALL

        -- AugustusSwapper1_0_event_Swapped
        SELECT
            swaps.block_timestamp	 AS block_time,
            swaps.user AS trader_a,
            NULL AS trader_b,
            swaps.srcAmount AS token_a_amount_raw,
            swaps.receivedAmount AS token_b_amount_raw,
            swaps.srcToken AS token_a_address,
            swaps.destToken AS token_b_address,
            swaps.contract_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper1_0_event_Swapped swaps
        UNION ALL

        -- AugustusSwapper2_0_event_Swapped
        -- AugustusSwapper3_0_event_Swapped
        -- AugustusSwapper4_0_event_Swapped
        -- AugustusSwapper5_0_event_Swapped
        -- AugustusSwapper3_0_event_Bought
        SELECT
            swaps.block_timestamp	 AS block_time,
            swaps.initiator AS trader_a,
            swaps.beneficiary AS trader_b,
            swaps.srcAmount AS token_a_amount_raw,
            swaps.receivedAmount AS token_b_amount_raw,
            swaps.srcToken AS token_a_address,
            swaps.destToken AS token_b_address,
            swaps.contract_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM(
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper2_0_event_Swapped UNION ALL
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper3_0_event_Swapped UNION ALL
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper4_0_event_Swapped UNION ALL
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper5_0_event_Swapped UNION ALL
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper3_0_event_Bought
        ) swaps
        UNION ALL

        -- AugustusSwapper4_0_event_Bought
        -- AugustusSwapper5_0_event_Bought
        SELECT
            swaps.block_timestamp	 AS block_time,
            swaps.initiator AS trader_a,
            swaps.beneficiary AS trader_b,
            swaps.srcAmount AS token_a_amount_raw,
            swaps.receivedAmount AS token_b_amount_raw,
            swaps.srcToken AS token_a_address,
            swaps.destToken AS token_b_address,
            swaps.contract_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM (
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper4_0_event_Bought UNION ALL
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper5_0_event_Bought
        ) swaps
        UNION ALL

        -- AugustusSwapper5_0_call_swapOnUniswap
        SELECT
            block_timestamp	 AS block_time,
            NULL AS trader_a,
            NULL AS trader_b,
            swaps.amountIn AS token_a_amount_raw,
            swaps.amountOutMin AS token_b_amount_raw,
            LOWER(SUBSTRING(path, 1, 42)) AS token_a_address,
            LOWER(LEFT(RIGHT(path, 42), 42)) AS token_b_address,
            swaps.to_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            swaps.trace_address AS trace_address,
            NULL AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper5_0_call_swapOnUniswap swaps
        UNION ALL

        -- AugustusSwapper5_0_call_swapOnUniswapFork
        SELECT
            block_timestamp	 AS block_time,
            NULL AS trader_a,
            NULL AS trader_b,
            swaps.amountIn AS token_a_amount_raw,
            swaps.amountOutMin AS token_b_amount_raw,
            LOWER(SUBSTRING(path, 1, 42)) AS token_a_address,
            LOWER(LEFT(RIGHT(path, 42), 42)) AS token_b_address,
            swaps.to_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            swaps.trace_address AS trace_address,
            NULL AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper5_0_call_swapOnUniswapFork swaps
        UNION ALL

        -- AugustusSwapper6_0_event_Swapped
        SELECT
            swaps.block_timestamp	 AS block_time,
            swaps.initiator AS trader_a,
            swaps.beneficiary AS trader_b,
            swaps.srcAmount AS token_a_amount_raw,
            swaps.receivedAmount AS token_b_amount_raw,
            swaps.srcToken AS token_a_address,
            swaps.destToken AS token_b_address,
            swaps.contract_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper6_0_event_Swapped swaps
        UNION ALL

        -- AugustusSwapper6_0_event_Bought
        SELECT
            swaps.block_timestamp	 AS block_time,
            swaps.initiator AS trader_a,
            swaps.beneficiary AS trader_b,
            swaps.srcAmount AS token_a_amount_raw,
            swaps.receivedAmount AS token_b_amount_raw,
            swaps.srcToken AS token_a_address,
            swaps.destToken AS token_b_address,
            swaps.contract_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper6_0_event_Bought swaps
        UNION ALL

        -- AugustusSwapper6_0_call_swapOnUniswap
        SELECT
            block_timestamp	 AS block_time,
            NULL AS trader_a,
            NULL AS trader_b,
            swaps.amountIn AS token_a_amount_raw,
            swaps.amountOutMin AS token_b_amount_raw,
            LOWER(SUBSTRING(path, 1, 42)) AS token_a_address,
            LOWER(LEFT(RIGHT(path, 42), 42)) AS token_b_address,
            swaps.to_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            swaps.trace_address AS trace_address,
            NULL AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper6_0_call_swapOnUniswap swaps
        UNION ALL

        -- AugustusSwapper6_0_call_swapOnUniswapFork
        SELECT
            block_timestamp	 AS block_time,
            NULL AS trader_a,
            NULL AS trader_b,
            swaps.amountIn AS token_a_amount_raw,
            swaps.amountOutMin AS token_b_amount_raw,
            LOWER(SUBSTRING(path, 1, 42)) AS token_a_address,
            LOWER(LEFT(RIGHT(path, 42), 42)) AS token_b_address,
            swaps.to_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            swaps.trace_address AS trace_address,
            NULL AS evt_index
        FROM footprint-etl.ethereum_paraswap.AugustusSwapper6_0_call_swapOnUniswapFork swaps
        UNION ALL

        -- AugustusSwapper6_0_call_swapOnZeroXv2
        -- AugustusSwapper6_0_call_swapOnZeroXv4
        SELECT
            block_timestamp	 AS block_time,
            NULL AS trader_a,
            NULL AS trader_b,
            swaps.fromAmount AS token_a_amount_raw,
            swaps.amountOutMin AS token_b_amount_raw,
            swaps.fromToken AS token_a_address,
            swaps.toToken AS token_b_address,
            swaps.to_address AS exchange_contract_address,
            swaps.transaction_hash AS tx_hash,
            swaps.trace_address AS trace_address,
            NULL AS evt_index
        FROM (
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper6_0_call_swapOnZeroXv2 UNION ALL
            SELECT * FROM footprint-etl.ethereum_paraswap.AugustusSwapper6_0_call_swapOnZeroXv4
        ) swaps

    ) dexs



