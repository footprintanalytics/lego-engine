SELECT  event.block_timestamp                                    AS block_time,
        'Ellipsis'                                               AS project,
        '1'                                                      AS version,
        'DEX'                                                    AS category,
        148                                                      AS protocol_id,
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
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.3EPS_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.DAI_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.DAI_swap_event_TokenExchangeUnderlying`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.TUSD_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.TUSD_swap_event_TokenExchangeUnderlying`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.USDN_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.USDN_swap_event_TokenExchangeUnderlying`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.anyBTC_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.anyBTC_swap_event_TokenExchangeUnderlying`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.btcEPS_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.fUSDT_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.fUSDT_swap_event_TokenExchangeUnderlying`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.pBTC_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.pBTC_swap_event_TokenExchangeUnderlying`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.UST_swap_event_TokenExchange`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_ellipsis.UST_swap_event_TokenExchangeUnderlying`
) event
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool
ON pool.pool_id = event.contract_address
WHERE DATE(event.block_timestamp) {match_date_filter}
