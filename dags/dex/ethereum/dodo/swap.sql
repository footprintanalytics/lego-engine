SELECT
        dexs.block_time,
        project,
        version,
        category,
        48 as protocol_id,
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

        -- dodo v1 sell
        SELECT
            s.block_timestamp AS block_time,
            'DODO' AS project,
            '1' AS version,
            'DEX' AS category,
            s.seller AS trader_a,
            NULL AS trader_b,
            s.payBase token_a_amount_raw,
            s.receiveQuote token_b_amount_raw,
            NULL AS usd_amount,
            m.base_token_address AS token_a_address,
            m.quote_token_address AS token_b_address,
            s.contract_address exchange_contract_address,
            s.transaction_hash AS tx_hash,
            NULL AS trace_address,
            s.log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DODOPool_event_SellBaseToken` s
        LEFT JOIN `footprint-etl.ethereum_dodo.view_markets` m on s.contract_address = m.market_contract_address
        WHERE s.seller <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodo v1 buy
        SELECT
            b.block_timestamp AS block_time,
            'DODO' AS project,
            '1' AS version,
            'DEX' AS category,
            b.buyer AS trader_a,
            NULL AS trader_b,
            b.receiveBase token_a_amount_raw,
            b.payQuote token_b_amount_raw,
            NULL AS usd_amount,
            m.base_token_address AS token_a_address,
            m.quote_token_address AS token_b_address,
            b.contract_address exchange_contract_address,
            b.transaction_hash AS tx_hash,
            NULL AS trace_address,
            b.log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DODOPool_event_BuyBaseToken` b
        LEFT JOIN `footprint-etl.ethereum_dodo.view_markets` m on b.contract_address = m.market_contract_address
        WHERE b.buyer <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodov1 proxy01
        SELECT
            block_timestamp AS block_time,
            'DODO' AS project,
            '1' AS version,
            'DEX' AS category,
            sender AS trader_a,
            NULL AS trader_b,
            fromAmount token_a_amount_raw,
            returnAmount token_b_amount_raw,
            NULL AS usd_amount,
            fromToken AS token_a_address,
            toToken AS token_b_address,
            contract_address exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DODOV1Proxy01_event_OrderHistory`

        UNION ALL

        -- -- dodov1 proxy04
        SELECT
            block_timestamp AS block_time,
            'DODO' AS project,
            '1' AS version,
            'DEX' AS category,
            sender AS trader_a,
            NULL AS trader_b,
            fromAmount token_a_amount_raw,
            returnAmount token_b_amount_raw,
            NULL AS usd_amount,
            fromToken AS token_a_address,
            toToken AS token_b_address,
            contract_address exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
         FROM
             `footprint-etl.ethereum_dodo.DODOV1Proxy04_event_OrderHistory`

         UNION ALL

        -- dodov2 proxy02
        SELECT
            block_timestamp AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            sender AS trader_a,
            NULL AS trader_b,
            fromAmount token_a_amount_raw,
            returnAmount token_b_amount_raw,
            NULL AS usd_amount,
            fromToken AS token_a_address,
            toToken AS token_b_address,
            contract_address exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DODOV2Proxy02_event_OrderHistory`

        UNION ALL

        -- dodov2 dvm
        SELECT
            block_timestamp AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            fromAmount token_a_amount_raw,
            toAmount token_b_amount_raw,
            NULL AS usd_amount,
            fromToken AS token_a_address,
            toToken AS token_b_address,
            contract_address exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DVM_event_DODOSwap`
        WHERE trader <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodov2 dpp
        SELECT
            block_timestamp AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            fromAmount AS token_a_amount_raw,
            toAmount AS token_b_amount_raw,
            NULL AS usd_amount,
            fromToken AS token_a_address,
            toToken AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DPP_event_DODOSwap`
        WHERE trader <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodov2 dsp
        SELECT
            block_timestamp AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            fromAmount AS token_a_amount_raw,
            toAmount AS token_b_amount_raw,
            NULL AS usd_amount,
            fromToken AS token_a_address,
            toToken AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_dodo.DSP_event_DODOSwap`
        WHERE trader <> '0xa356867fdcea8e71aeaf87805808803806231fdc'
    ) dexs


    