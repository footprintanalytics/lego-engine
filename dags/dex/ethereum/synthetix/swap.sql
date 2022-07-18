SELECT
        tr.block_time,
        token_a_amount,
        token_b_amount,
        'Synthetix' AS project,
        '1' AS version,
        'DEX' AS category,
        11 as protocol_id,
        trader_a,
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        Lower(token_a_address) AS token_a_address,
        Lower(token_b_address) AS token_b_address,
        exchange_contract_address,
        tx_hash,
        NULL AS trace_address,
        evt_index
FROM
(
        with synths as(
    select * from `footprint-etl.ethereum_synthetix.synths` ORDER BY TIMESTAMP(evt_block_time) DESC
)
SELECT
            block_timestamp AS block_time,
            CAST((fromAmount) AS float64)/1e18 AS token_a_amount,
            CAST((toAmount) AS float64)/1e18 AS token_b_amount,
            'Synthetix' AS project,
            '1' AS version,
            account AS trader_a,
            NULL AS trader_b,
            fromAmount AS token_a_amount_raw,
            toAmount AS token_b_amount_raw,
            ARRAY(SELECT address FROM synths WHERE trade.fromCurrencyKey = currency_key AND TIMESTAMP(evt_block_time) <= trade.block_timestamp  )[SAFE_OFFSET(0)] AS token_a_address,
            ARRAY(SELECT address FROM synths WHERE trade.toCurrencyKey = currency_key AND TIMESTAMP(evt_block_time) <= trade.block_timestamp )[SAFE_OFFSET(0)] AS token_b_address,
            contract_address AS exchange_contract_address,
            transaction_hash AS tx_hash,
            NULL AS trace_address,
            log_index AS evt_index
        FROM
            `footprint-etl.ethereum_synthetix.Synthetix_event_SynthExchange` trade
) tr