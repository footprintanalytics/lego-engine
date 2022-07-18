WITH
    view_api_affiliate_data AS (
        SELECT
            tr.transaction_hash
            , tr.trace_address
            , tr.block_number
            , tr.block_timestamp
            , tr.from_address AS caller
            , tr.to_address AS callee
            ,CASE
                WHEN CONTAINS_SUBSTR(input, '869584cd') THEN concat('0x', SUBSTRING(input, (STRPOS(input, '869584cd') + 32), 40))
                WHEN CONTAINS_SUBSTR(input, 'fbc019a7') THEN concat('0x', SUBSTRING(input, (STRPOS(input, 'fbc019a7') + 32), 40))
                END AS affiliate_address
            , NULL AS quote_timestamp
        FROM `bigquery-public-data.crypto_ethereum.traces` tr
        WHERE
            tr.to_address IN (
                -- exchange contract
                '0x61935cbdd02287b511119ddb11aeb42f1593b7ef'
                -- forwarder addresses
                , '0x6958f5e95332d93d21af0d7b9ca85b8212fee0a5'
                , '0x4aa817c6f383c8e8ae77301d18ce48efb16fd2be'
                , '0x4ef40d1bf0983899892946830abf99eca2dbc5ce'
                -- exchange proxy
                , '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
            )
        AND (
            CONTAINS_SUBSTR(input, '869584cd')
            OR
            CONTAINS_SUBSTR(input, 'fbc019a7')
        )
        AND
            Date(tr.block_timestamp) {match_date_filter}
    ),
    zeroex_tx_raw AS (
        SELECT DISTINCT
            v3.transaction_hash AS tx_hash,
            case when takerAddress = '0x63305728359c088a52b0b0eeec235db4d31a67fc' then takerAddress
                else null
                end as affiliate_address
        FROM `footprint-etl.ethereum_zeroex.Exchange_event_Fill` v3
        WHERE
            -- nuo
            v3.takerAddress = '0x63305728359c088a52b0b0eeec235db4d31a67fc'
            OR
            -- contains a bridge order
            (v3.feeRecipientAddress = '0x1000000000000000000000000000000000000011'
            AND SUBSTRING(v3.makerAssetData,1,10) = '0xdc1600f3')

        UNION DISTINCT
        SELECT
            transaction_hash,
            affiliate_address as affiliate_address
        from view_api_affiliate_data
    ),
    zeroex_tx AS (
        SELECT
            tx_hash,
            MAX(affiliate_address) as affiliate_address
        from zeroex_tx_raw
        GROUP BY 1
    ),
    v3_fills_no_bridge AS (
        SELECT fills.transaction_hash AS tx_hash
            , fills.log_index AS evt_index
            , fills.contract_address
            , block_timestamp AS block_time
            , fills.makerAddress AS maker
            , fills.takerAddress AS taker
            , case when CHAR_LENGTH(SUBSTRING(fills.takerAssetData,35, 40)) > 0 then concat('0x', SUBSTRING(fills.takerAssetData,35, 40)) end AS taker_token
            , case when CHAR_LENGTH(SUBSTRING(fills.makerAssetData,35, 40)) > 0 then concat('0x', SUBSTRING(fills.makerAssetData,35, 40)) end AS maker_token
            , fills.takerAssetFilledAmount  AS taker_token_amount_raw
            , fills.makerAssetFilledAmount  AS maker_token_amount_raw
            , 'Native Fill v3' as type
            , COALESCE(zeroex_tx.affiliate_address, fills.feeRecipientAddress) as affiliate_address
            , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
            , (fills.feeRecipientAddress = '0x86003b044f70dac0abc80ac8957305b6370893ed') AS matcha_limit_order_flag
        FROM `footprint-etl.ethereum_zeroex.Exchange_event_Fill` fills
        LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.transaction_hash
        WHERE
            (SUBSTRING('makerAssetData',1,10) != '0xdc1600f3')
            AND (
                zeroex_tx.tx_hash IS NOT NULL
                OR fills.feeRecipientAddress = '0x86003b044f70dac0abc80ac8957305b6370893ed'
            )
    ),
    v4_rfq_fills_no_bridge AS (
        SELECT fills.transaction_hash AS tx_hash
            , fills.log_index AS evt_index
            , fills.contract_address
            , fills.block_timestamp AS block_time
            , fills.maker AS maker
            , fills.taker AS taker
            , fills.takerToken AS taker_token
            , fills.makerToken AS maker_token
            , fills.takerTokenFilledAmount  AS taker_token_amount_raw
            , fills.makerTokenFilledAmount  AS maker_token_amount_raw
            , 'Native Fill v4' as type
            , zeroex_tx.affiliate_address as affiliate_address
            , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
            , FALSE AS matcha_limit_order_flag
        FROM `footprint-etl.ethereum_zeroex.MultiplexFeature_event_RfqOrderFilled` fills
        LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.transaction_hash
    ),
    v4_limit_fills_no_bridge AS (
        SELECT fills.transaction_hash AS tx_hash
            , fills.log_index AS evt_index
            , fills.contract_address
            , fills.block_timestamp AS block_time
            , fills.maker AS maker
            , fills.taker AS taker
            , fills.takerToken AS taker_token
            , fills.makerToken AS maker_token
            , fills.takerTokenFilledAmount AS taker_token_amount_raw
            , fills.makerTokenFilledAmount AS maker_token_amount_raw
            , 'Native Fill v4' as type
            , COALESCE(zeroex_tx.affiliate_address, fills.feeRecipient) as affiliate_address
            , (zeroex_tx.tx_hash IS NOT NULL) AS swap_flag
            , (fills.feeRecipient = '0x86003b044f70dac0abc80ac8957305b6370893ed') AS matcha_limit_order_flag
        FROM `footprint-etl.ethereum_zeroex.MultiplexFeature_event_LimitOrderFilled` fills
        LEFT join zeroex_tx on zeroex_tx.tx_hash = fills.transaction_hash
    ),
    -- bridge fills
    ERC20BridgeTransfer AS (
        SELECT 	logs.transaction_hash,
                logs.log_index AS evt_index,
                logs.address,
                block_timestamp AS block_time,
                case when CHAR_LENGTH(substring(DATA,283,40)) > 0 then concat('0x', substring(DATA,283,40)) end AS maker,
                case when CHAR_LENGTH(substring(DATA,347,40)) > 0 then concat('0x', substring(DATA,347,40)) end AS taker,
                case when CHAR_LENGTH(substring(DATA,27,40)) > 0 then concat('0x', substring(DATA,27,40)) end AS taker_token,
                case when CHAR_LENGTH(substr(DATA,91,40)) > 0 then concat('0x', substring(DATA,91,40)) end AS maker_token,
                case when CHAR_LENGTH(substr(DATA,155,40)) > 0 then cast(cast(concat('0x', substr(DATA,155,40)) as Float64) as string) end as taker_token_amount_raw,
                case when CHAR_LENGTH(substr(DATA,219,40)) > 0 then cast(cast(concat('0x', substr(DATA,219,40)) as Float64) as string) end as maker_token_amount_raw,
                'Bridge Fill' AS type,
                zeroex_tx.affiliate_address as affiliate_address,
                TRUE AS swap_flag,
                FALSE AS matcha_limit_order_flag
        FROM `bigquery-public-data.crypto_ethereum.logs` logs
        JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.transaction_hash
        WHERE ARRAY_LENGTH(topics) > 0
        AND topics[offset(0)] = '0x349fc08071558d8e3aa92dec9396e4e9f2dfecd6bb9065759d1932e7da43b8a9'
        AND Date(block_timestamp) {match_date_filter}
    ),

    BridgeFill AS (
        SELECT 	logs.transaction_hash,
                logs.log_index AS evt_index,
                logs.address,
                block_timestamp AS block_time,
                case when CHAR_LENGTH(substring(DATA,27,40)) > 0 then concat('0x', substring(DATA,27,40)) end AS maker,
                '0xdef1c0ded9bec7f1a1670819833240f027b25eff' AS taker,
                case when CHAR_LENGTH(substr(DATA,91,40)) > 0 then concat('0x', substring(DATA,91,40)) end AS taker_token,
                case when CHAR_LENGTH(substr(DATA,155,40)) > 0 then concat('0x', substring(DATA,155,40)) end AS maker_token,
                case when CHAR_LENGTH(substr(DATA,219,40)) > 0 then cast(cast(concat('0x', substr(DATA,219,40)) as Float64) as string) end as taker_token_amount_raw,
                case when CHAR_LENGTH(substr(DATA,283,40)) > 0 then cast(cast(concat('0x', substr(DATA,283,40)) as Float64) as string) end as maker_token_amount_raw,
                'Bridge Fill' AS type,
                zeroex_tx.affiliate_address as affiliate_address,
                TRUE AS swap_flag,
                FALSE AS matcha_limit_order_flag
        FROM `bigquery-public-data.crypto_ethereum.logs` logs
        JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.transaction_hash
        WHERE ARRAY_LENGTH(topics) > 0
        AND topics[offset(0)] = '0xff3bc5e46464411f331d1b093e1587d2d1aa667f5618f98a95afc4132709d3a9'
        AND Date(block_timestamp) {match_date_filter}
    ),

    NewBridgeFill AS (
        SELECT  logs.transaction_hash,
            logs.log_index AS evt_index,
            logs.address,
            block_timestamp AS block_time,
            case when CHAR_LENGTH(substring(DATA,27,40)) > 0 then concat('0x', substring(DATA,27,40)) end AS maker,
            '0xdef1c0ded9bec7f1a1670819833240f027b25eff' AS taker,
            case when CHAR_LENGTH(substr(DATA,91,40)) > 0 then concat('0x', substring(DATA,91,40)) end AS taker_token,
            case when CHAR_LENGTH(substr(DATA,155,40)) > 0 then concat('0x', substring(DATA,155,40)) end AS maker_token,
            case when CHAR_LENGTH(substr(DATA,219,40)) > 0 then cast(cast(concat('0x', substr(DATA,219,40)) as Float64) as string) end as taker_token_amount_raw,
            case when CHAR_LENGTH(substr(DATA,283,40)) > 0 then cast(cast(concat('0x', substr(DATA,283,40)) as Float64) as string) end as maker_token_amount_raw,
            'Bridge Fill' AS type,
            zeroex_tx.affiliate_address as affiliate_address,
            TRUE AS swap_flag,
            FALSE AS matcha_limit_order_flag
        FROM `bigquery-public-data.crypto_ethereum.logs` logs
        JOIN zeroex_tx ON zeroex_tx.tx_hash = logs.transaction_hash
        WHERE ARRAY_LENGTH(topics) > 0
        AND topics[offset(0)] = '0xe59e71a14fe90157eedc866c4f8c767d3943d6b6b2e8cd64dddcc92ab4c55af8'
        AND address = '0x22f9dcf4647084d6c31b2765f6910cd85c178c18'
    ),
    direct_PLP AS (
        SELECT 	plp.transaction_hash,
                plp.log_index AS evt_index,
                plp.contract_address,
                plp.block_timestamp AS block_time,
                provider AS maker,
                recipient AS taker,
                plp.inputToken AS taker_token,
                outputToken AS maker_token,
                inputTokenAmount AS taker_token_amount_raw,
                outputTokenAmount AS maker_token_amount_raw,
                'PLP Direct' AS type,
                zeroex_tx.affiliate_address as affiliate_address,
        TRUE AS swap_flag,
        FALSE AS matcha_limit_order_flag
        FROM `footprint-etl.ethereum_zeroex.MultiplexFeature_event_LiquidityProviderSwap` plp
        JOIN zeroex_tx ON zeroex_tx.tx_hash = plp.transaction_hash
    ),

    direct_uniswapv2 AS (
        SELECT 	swap.transaction_hash AS tx_hash,
                swap.log_index AS evt_index,
                swap.contract_address,
                swap.block_timestamp AS block_time,
                swap.contract_address AS maker,
                LAST_VALUE(swap.to) OVER (PARTITION BY swap.transaction_hash ORDER BY swap.log_index) AS taker,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN pair.token0
                    ELSE pair.token1
                    END AS taker_token,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN pair.token1
                    ELSE pair.token0
                    END AS maker_token,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN swap.amount0In
                    ELSE swap.amount1In
                    END AS taker_token_amount_raw,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN swap.amount1Out
                    ELSE swap.amount0Out
                    END AS maker_token_amount_raw,
                'UniswapV2 Direct' AS type,
                zeroex_tx.affiliate_address as affiliate_address,
                TRUE AS swap_flag,
                FALSE AS matcha_limit_order_flag
        FROM `footprint-etl.ethereum_uniswap.UniswapV2Pair_event_Swap` swap
        LEFT JOIN `footprint-etl.ethereum_uniswap.UniswapV2Factory_event_PairCreated` pair ON pair.pair = swap.contract_address
        join zeroex_tx on zeroex_tx.tx_hash = swap.transaction_hash
        WHERE sender = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
    ),

    direct_sushiswap AS (
        SELECT 	swap.transaction_hash AS tx_hash,
                swap.log_index AS evt_index,
                swap.contract_address,
                swap.block_timestamp AS block_time,
                swap.contract_address AS maker,
                LAST_VALUE(swap.to) OVER (PARTITION BY swap.transaction_hash ORDER BY swap.log_index) AS taker,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN pair.token0
                    ELSE pair.token1
                    END AS taker_token,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN pair.token1
                    ELSE pair.token0
                    END AS maker_token,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN swap.amount0In
                    ELSE swap.amount1In
                    END AS taker_token_amount_raw,
                CASE
                    WHEN swap.amount0In > swap.amount1In THEN swap.amount1Out
                    ELSE swap.amount0Out
                    END AS maker_token_amount_raw,
                'Sushiswap Direct' AS type,
                zeroex_tx.affiliate_address as affiliate_address,
                TRUE AS swap_flag,
                FALSE AS matcha_limit_order_flag
        FROM `footprint-etl.ethereum_sushi.Pair_event_Swap` swap
        LEFT JOIN `footprint-etl.ethereum_sushi.Factory_event_PairCreated` pair ON pair.pair = swap.contract_address
        join zeroex_tx on zeroex_tx.tx_hash = swap.transaction_hash
        WHERE sender = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
    ),

    direct_uniswapv3 AS (
        SELECT 	swap.transaction_hash AS tx_hash,
                swap.log_index as evt_index,
        swap.contract_address,
                swap.block_timestamp AS block_time,
                swap.contract_address AS maker,
                LAST_VALUE(swap.recipient) OVER (PARTITION BY swap.log_index ORDER BY swap.log_index) AS taker,
                pair.token1 AS taker_token,
                pair.token0 AS maker_token,
        cast(abs(cast(swap.amount1 as FLOAT64)) as STRING) AS taker_token_amount_raw,
        cast(abs(cast(swap.amount0 as FLOAT64)) as string) AS maker_token_amount_raw,
        'UniswapV3 Direct' AS type,
        zeroex_tx.affiliate_address as affiliate_address,
        TRUE AS swap_flag,
        FALSE AS matcha_limit_order_flag
        FROM `blockchain-etl.ethereum_uniswap.UniswapV3Pool_event_Swap` swap
        LEFT JOIN `blockchain-etl.ethereum_uniswap.UniswapV3Factory_event_PoolCreated` pair ON pair.pool = swap.contract_address
    join zeroex_tx on zeroex_tx.tx_hash = swap.transaction_hash
        WHERE sender = '0xdef1c0ded9bec7f1a1670819833240f027b25eff'
    ),

    all_tx AS (
        SELECT * FROM direct_uniswapv2
        UNION ALL
        SELECT * FROM direct_uniswapv3
        UNION ALL
        SELECT * FROM direct_sushiswap
        UNION ALL
        SELECT * FROM direct_PLP
        UNION ALL
        SELECT * FROM ERC20BridgeTransfer
        UNION ALL
        SELECT * FROM BridgeFill
        UNION ALL
        SELECT * FROM NewBridgeFill
        UNION ALL
        SELECT * FROM v3_fills_no_bridge
        UNION ALL
        SELECT * FROM v4_rfq_fills_no_bridge
        UNION ALL
        SELECT * FROM v4_limit_fills_no_bridge
    ),
    total_volume AS (
        SELECT 	all_tx.tx_hash,
                all_tx.evt_index,
                all_tx.contract_address,
                all_tx.block_time,
                maker,
                case when taker = '0xdef1c0ded9bec7f1a1670819833240f027b25eff' then tx.from_address else taker end as taker, -- fix the user masked by ProxyContract issue
                taker_token,
                maker_token,
                -- taker_token_amount_raw / (10^tt.decimals) AS taker_token_amount,
            taker_token_amount_raw,
            -- maker_token_amount_raw / (10^mt.decimals) AS maker_token_amount,
            maker_token_amount_raw,
                all_tx.type,
            affiliate_address,
            swap_flag,
            matcha_limit_order_flag
        FROM all_tx
        INNER JOIN `bigquery-public-data.crypto_ethereum.transactions`  tx ON all_tx.tx_hash = tx.hash
        WHERE DATE(tx.block_timestamp) {match_date_filter}
    )



-- ************************************

SELECT
    dexs.block_time,
    481 as protocol_id,
    project,
    '1' as version,
    category,
    trader_a,
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
    row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address , category) AS trade_id
    FROM (
        -- 0x v2.1
        SELECT
            block_timestamp AS block_time,
            '0x Native' AS project,
            'DEX' AS category,
            takerAddress AS trader_a,
            makerAddress AS trader_b,
            takerAssetFilledAmount AS token_a_amount_raw,
            makerAssetFilledAmount AS token_b_amount_raw,
            NULL AS usd_amount,
            concat('0x', SUBSTR(takerAssetData, 35, 40)) AS token_a_address,
            concat('0x', SUBSTR(makerAssetData, 35, 40)) AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM `footprint-etl.ethereum_zeroex.Exchange2_1_event_Fill`

        UNION ALL

        -- 0x v3
        SELECT
            block_timestamp AS block_time,
            '0x Native' AS project,
            'DEX' AS category,
            takerAddress AS trader_a,
            makerAddress AS trader_b,
            takerAssetFilledAmount AS token_a_amount_raw,
            makerAssetFilledAmount AS token_b_amount_raw,
            NULL AS usd_amount,
            concat('0x', SUBSTR(takerAssetData, 35, 40)) AS token_a_address,
            concat('0x', SUBSTR(makerAssetData, 35, 40)) AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM `footprint-etl.ethereum_zeroex.Exchange_event_Fill`

        UNION ALL

        -- 0x v4 limit orders
        SELECT
            block_timestamp AS block_time,
            '0x Native' AS project,
            'DEX' AS category,
            taker AS trader_a,
            maker AS trader_b,
            takerTokenFilledAmount AS token_a_amount_raw,
            makerTokenFilledAmount AS token_b_amount_raw,
            NULL AS usd_amount,
            takerToken AS token_a_address,
            makerToken AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM `footprint-etl.ethereum_zeroex.MultiplexFeature_event_LimitOrderFilled`

        UNION ALL

        -- 0x v4 rfq orders
        SELECT
            block_timestamp AS block_time,
            '0x Native' AS project,
            'DEX' AS category,
            taker AS trader_a,
            maker AS trader_b,
            takerTokenFilledAmount AS token_a_amount_raw,
            makerTokenFilledAmount AS token_b_amount_raw,
            NULL AS usd_amount,
            takerToken AS token_a_address,
            makerToken AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM `footprint-etl.ethereum_zeroex.MultiplexFeature_event_RfqOrderFilled`

        UNION ALL

        -- 0x api
        SELECT
            block_time,
            '0x API' AS project,
            'Aggregator' AS category,
            taker AS trader_a,
            maker AS trader_b,
            taker_token_amount_raw AS token_a_amount_raw,
            maker_token_amount_raw AS token_b_amount_raw,
            NULL AS usd_amount,
            taker_token AS token_a_address,
            maker_token AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            evt_index,
            NULL AS trace_address,

        FROM total_volume
        where swap_flag is TRUE

        UNION ALL

        -- Matcha
        SELECT
            block_time,
            'Matcha' AS project,
            'Aggregator' AS category,
            taker AS trader_a,
            maker AS trader_b,
            taker_token_amount_raw AS token_a_amount_raw,
            maker_token_amount_raw AS token_b_amount_raw,
            NULL AS usd_amount,
            taker_token AS token_a_address,
            maker_token AS token_b_address,
            contract_address AS exchange_contract_address,
            tx_hash,
            NULL AS trace_address,
            evt_index
        FROM total_volume
        where affiliate_address ='0x86003b044f70dac0abc80ac8957305b6370893ed'
) dexs
where DATE(block_time) {match_date_filter}

