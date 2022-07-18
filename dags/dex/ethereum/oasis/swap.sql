SELECT
        dexs.block_time,
        project,
        version,
        category,
        451 as protocol_id,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        null as usd_amount,
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
            'Oasis' AS project,
            '1' AS version,
            'DEX' AS category,
            take.taker AS trader_a,
            take.maker AS trader_b,
            CAST(t.buy_amt AS FLOAT64) AS token_a_amount_raw,
            CAST(t.pay_amt AS FLOAT64) AS token_b_amount_raw,
            NULL AS usd_amount,
            t.buy_gem AS token_a_address,
            t.pay_gem AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index AS evt_index
        FROM `footprint-etl.ethereum_oasis.eth2dai_event_LogTrade` t
        LEFT JOIN `footprint-etl.ethereum_oasis.eth2dai_event_LogTake` take
        ON t.transaction_hash = take.transaction_hash AND take.log_index > t.log_index

        UNION ALL

        -- Oasis contract
        SELECT
            t.block_timestamp AS block_time,
            'Oasis' AS project,
            '2' AS version,
            'DEX' AS category,
            take.taker AS trader_a,
            take.maker AS trader_b,
            CAST(t.buy_amt AS FLOAT64) AS token_a_amount_raw,
            CAST(t.pay_amt AS FLOAT64) AS token_b_amount_raw,
            NULL AS usd_amount,
            t.buy_gem AS token_a_address,
            t.pay_gem AS token_b_address,
            t.contract_address AS exchange_contract_address,
            t.transaction_hash AS tx_hash,
            NULL AS trace_address,
            t.log_index AS evt_index
        FROM `footprint-etl.ethereum_oasis.MatchingMarket_event_LogTrade` t
        LEFT JOIN  `footprint-etl.ethereum_oasis.MatchingMarket_event_LogTake` take
        ON t.transaction_hash = take.transaction_hash AND take.log_index > t.log_index
    ) dexs