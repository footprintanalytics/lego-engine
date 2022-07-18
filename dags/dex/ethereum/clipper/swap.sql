with view_trades as (
 SELECT
    block_timestamp AS block_time,
    'Clipper' AS project,
    '1' AS version,
    recipient AS trader_a,
    NULL AS trader_b,
    inAmount AS token_a_amount_raw,
    outAmount AS token_b_amount_raw,
    inAsset as token_a_address,
    outAsset as token_b_address,
    contract_address AS exchange_contract_address,
    transaction_hash AS tx_hash,
    NULL AS trace_address,
    log_index
FROM footprint-etl.ethereum_clipper.ClipperExchangeInterface_event_Swapped

)


SELECT
        dexs.block_time,
                        project,
        version,
        category,
        805 as protocol_id,
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
        -- Clipper
        SELECT
            block_time,
            project,
            version,
            'DEX' as category,
            trader_a,
            trader_b,
            token_a_amount_raw,
            token_b_amount_raw,
            NULL AS usd_amount,
            -- Cast ETH as WETH for price purposes
            CASE WHEN token_a_address = '0x0000000000000000000000000000000000000000' THEN
                '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE token_a_address
            END AS token_a_address,
            CASE WHEN token_b_address = '0x0000000000000000000000000000000000000000' THEN
                '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ELSE token_b_address
            END AS token_b_address,
            exchange_contract_address,
            tx_hash,
            trace_address,
            log_index AS evt_index
        FROM view_trades WHERE project='Clipper'
    ) dexs

    