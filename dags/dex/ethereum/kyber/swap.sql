SELECT
        dexs.block_time,
        project,
        version,
        category,
        28 as protocol_id,
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
            block_timestamp AS block_time,
            'Kyber' AS project,
            '1' AS version,
            'DEX' AS category,
            trader AS trader_a,
            NULL AS trader_b,
            CAST(CASE
                WHEN src in ('0x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN '0'
                ELSE srcAmount
            END AS FLOAT64) AS token_a_amount_raw,
            CAST(ethWeiValue AS FLOAT64) AS token_b_amount_raw,
            NULL AS usd_amount,
            src AS token_a_address,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address,
            contract_address exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_kyber.Network_event_KyberTrade`
        WHERE src NOT IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

        UNION ALL

        SELECT
            block_timestamp AS block_time,
            'Kyber' AS project,
            '1' AS version,
            'DEX' AS category,
            trader AS trader_a,
            NULL AS trader_b,
            CAST(ethWeiValue AS FLOAT64) AS token_a_amount_raw,
            CAST(CASE
                WHEN dest IN ('0x5228a22e72ccc52d415ecfd199f99d0665e7733b') THEN '0'
                ELSE dstAmount
            END AS FLOAT64) AS token_b_amount_raw,
            NULL AS usd_amount,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address,
            dest AS token_b_address,
            contract_address exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_kyber.Network_event_KyberTrade`
        WHERE dest NOT IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')

        UNION ALL

        --- Kyber_V2
        -- trade from token -eth
        SELECT
            block_timestamp AS block_time,
            'Kyber' AS project,
            '2' AS version,
            'DEX' AS category,
            NULL AS trader_a,
            NULL AS trader_b,

            (SELECT SUM(s) FROM UNNEST(ARRAY((
                SELECT
                    CASE WHEN 18 >= src_token.decimals -- eth decimal
                        THEN a*b*power(10, (18-src_token.decimals))/1e18
                        ELSE (a*b) / (1e18 * power(10, (src_token.decimals - 18)))
                    END
                -- FROM unnest("t2eSrcAmounts", "t2eRates") AS t(a,b)
                    FROM (
                        select CAST(aTable.a AS FLOAT64) as a, CAST(bTable.b AS FLOAT64) as b from
                        (SELECT A as a, row_number()over() as row from unnest(t2eSrcAmounts) AS A) as aTable
                        left join
                        (SELECT B as b, row_number()over() as row from unnest(t2eRates) AS B) as bTable
                        ON aTable.row = bTable.row
                    )
                ))) s
            ) AS token_a_amount_raw,

            (SELECT SUM(CAST(a AS FLOAT64)) FROM UNNEST(t2eSrcAmounts) AS a) AS token_b_amount_raw,
            NULL AS usd_amount,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_a_address,
            src AS token_b_address,
            trade.contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index

        FROM `footprint-etl.ethereum_kyber.Network_v2_event_KyberTrade` trade
        INNER JOIN `xed-project-237404.footprint_etl.erc20_tokens` src_token ON trade.src = src_token.contract_address
        AND src_token.contract_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'

        UNION ALL

        -- trade from eth - token
        SELECT
            block_timestamp AS block_time,
            'Kyber' AS project,
            '2' AS version,
            'DEX' AS category,
            NULL AS trader_a,
            NULL AS trader_b,

            (SELECT SUM(s) FROM UNNEST(ARRAY((
                SELECT
                    CASE WHEN dst_token.decimals >= 18 -- eth decimal
                        THEN a*b*power(10, (dst_token.decimals-18))/1e18
                        ELSE (a*b) / (1e18 * power(10, (18- dst_token.decimals)))
                    END
                -- FROM unnest("e2tSrcAmounts", "e2tRates") AS t(a,b)
                    FROM (
                        select CAST(aTable.a AS FLOAT64) as a, CAST(bTable.b AS FLOAT64) as b from
                        (SELECT A as a, row_number()over() as row from unnest(e2tSrcAmounts) AS A) as aTable
                        left join
                        (SELECT B as b, row_number()over() as row from unnest(e2tRates) AS B) as bTable
                        ON aTable.row = bTable.row
                    )
                ))) s
            ) AS token_a_amount_raw,

            (SELECT SUM(CAST(a AS FLOAT64)) FROM UNNEST(e2tSrcAmounts) AS a) AS token_b_amount_raw,
            NULL AS usd_amount,
            dest AS token_a_address,
            '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS token_b_address,
            trade.contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM `footprint-etl.ethereum_kyber.Network_v2_event_KyberTrade` trade
        INNER JOIN `xed-project-237404.footprint_etl.erc20_tokens` dst_token ON trade.dest = dst_token.contract_address
        AND dst_token.contract_address != '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    ) dexs