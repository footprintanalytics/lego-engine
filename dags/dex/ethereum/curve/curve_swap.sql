-- TokenExchange
SELECT  event.block_timestamp                                    AS block_time,
        'Curve'                                                  AS project,
        event.version                                            AS version,
        'DEX'                                                    AS category,
        3                                                        AS protocol_id,
        event.buyer                                              AS trader_a,
        NULL                                                     AS trader_b,
        event.tokens_bought                                      AS token_a_amount_raw,
        event.tokens_sold                                        AS token_b_amount_raw,
        NULL                                                     AS usd_amount,
        pool.stake_token[OFFSET(CAST(event.bought_id AS INT64))] AS token_a_address,
        pool.stake_token[OFFSET(CAST(event.sold_id AS INT64))]   AS token_b_address,
        event.contract_address                                   AS exchange_contract_address,
        event.transaction_hash                                   AS tx_hash,
        NULL                                                     AS trace_address,
        event.log_index
FROM
(
              SELECT *, '1' AS version FROM `footprint-etl.ethereum_curve.swap_2coin_v1_event_TokenExchange`
    UNION ALL SELECT *, '1' AS version FROM `footprint-etl.ethereum_curve.swap_3coin_v1_event_TokenExchange`
    UNION ALL SELECT *, '1' AS version FROM `footprint-etl.ethereum_curve.swap_4coin_v1_event_TokenExchange`
    UNION ALL SELECT *, '2' AS version FROM `footprint-etl.ethereum_curve.swap_2coin_v2_event_TokenExchange`
    UNION ALL SELECT *, '2' AS version FROM `footprint-etl.ethereum_curve.swap_3coin_v2_event_TokenExchange`
) event
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool
ON pool.pool_id = event.contract_address
WHERE DATE(event.block_timestamp) {match_date_filter}
UNION ALL
-- TokenExchangeUnderlying
SELECT  event.block_timestamp                                               AS block_time,
        'Curve'                                                             AS project,
        event.version                                                       AS version,
        'DEX'                                                               AS category,
        3                                                                   AS protocol_id,
        event.buyer                                                         AS trader_a,
        NULL                                                                AS trader_b,
        event.tokens_bought                                                 AS token_a_amount_raw,
        event.tokens_sold                                                   AS token_b_amount_raw,
        NULL                                                                AS usd_amount,
        pool.stake_underlying_token[OFFSET(CAST(event.bought_id AS INT64))] AS token_a_address,
        pool.stake_underlying_token[OFFSET(CAST(event.sold_id AS INT64))]   AS token_b_address,
        event.contract_address                                              AS exchange_contract_address,
        event.transaction_hash                                              AS tx_hash,
        NULL                                                                AS trace_address,
        event.log_index
FROM
(
              SELECT *, '1' AS version FROM `footprint-etl.ethereum_curve.swap_2coin_v1_event_TokenExchangeUnderlying`
    UNION ALL SELECT *, '1' AS version FROM `footprint-etl.ethereum_curve.swap_3coin_v1_event_TokenExchangeUnderlying`
    UNION ALL SELECT *, '1' AS version FROM `footprint-etl.ethereum_curve.swap_4coin_v1_event_TokenExchangeUnderlying`
) event
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool
ON pool.pool_id = event.contract_address
WHERE DATE(event.block_timestamp) {match_date_filter}
