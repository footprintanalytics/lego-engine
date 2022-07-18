SELECT
        dexs.block_time,
                        project,
        version,
        category,
        86 as protocol_id,
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
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (
        SELECT
            t.block_timestamp AS block_time,
            'LINKSWAP' AS project,
            '1' AS version,
            'DEX' AS category,
            t.to AS trader_a,
            NULL AS trader_b,
            CASE WHEN amount0Out = '0' THEN amount1Out ELSE amount0Out END AS token_a_amount_raw,
            CASE WHEN amount0In = '0' THEN amount1In ELSE amount0In END AS token_b_amount_raw,
            NULL AS usd_amount,
            CASE WHEN amount0Out = '0' THEN f.token1 ELSE f.token0 END AS token_a_address,
            CASE WHEN amount0In = '0' THEN f.token1 ELSE f.token0 END AS token_b_address,
            t.contract_address exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index  AS evt_index
        FROM
            footprint-etl.ethereum_yflink.pair_event_Swap t
        INNER JOIN footprint-etl.ethereum_yflink.factory_event_PairCreated f ON f.pair = t.contract_address
    ) dexs


    