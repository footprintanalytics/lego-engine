SELECT
        dexs.block_time,
                        project,
        version,
        category,
        139 as protocol_id,
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
        -- IDEX v1
        SELECT
            block_timestamp AS block_time,
            'IDEX' AS project,
            '1' AS version,
            'DEX' AS category,
            tradeAddresses[safe_ORDINAL(3)] AS trader_a,
            tradeAddresses[safe_ORDINAL(4)] AS trader_b,
            tradeValues[safe_ORDINAL(1)] AS token_a_amount_raw,
            tradeValues[safe_ORDINAL(2)] AS token_b_amount_raw,
            NULL AS usd_amount,
            CASE WHEN tradeAddresses[safe_ORDINAL(1)] = '0x0000000000000000000000000000000000000000' THEN
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'             ELSE lower(tradeAddresses[safe_ORDINAL(1)])
            END AS token_a_address,
            CASE WHEN tradeAddresses[safe_ORDINAL(2)] = '0x0000000000000000000000000000000000000000' THEN
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'             ELSE lower(tradeAddresses[safe_ORDINAL(2)])
            END AS token_b_address,
            to_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            trace_address,
            NULL AS evt_index
        FROM `blockchain-etl.ethereum_idex.Exchange_call_trade`
        WHERE status = 1
    ) dexs

    