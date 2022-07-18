SELECT  'Curve'                                                      AS project,
        event.provider                                               AS liquidity_provider,
        event.version                                                AS version,
        3                                                            AS protocol_id,
        token.symbol                                                 AS token_symbol,
        CAST(event.token_amount AS FLOAT64) / POW(10,token.decimals) AS token_amount,
        NULL                                                         AS usd_value_of_token,
        CAST(event.token_amount AS FLOAT64)                          AS token_amount_raw,
        'AMM'                                                        AS type,
        event.token_address,
        event.contract_address                                       AS exchange_address,
        event.transaction_hash                                       AS tx_hash,
        DATETIME(event.block_timestamp)                              AS block_time,
        transaction.from_address                                     AS tx_from
FROM
(
    SELECT  *,
            pool.stake_token[OFFSET(CAST(token_amount_index AS INT64))] AS token_address
    FROM
    (
                  SELECT provider, '1' AS version, SPLIT(token_amounts) AS token_amounts_list, contract_address, transaction_hash, block_timestamp FROM `footprint-etl.ethereum_curve.swap_2coin_v1_event_AddLiquidity`
        UNION ALL SELECT provider, '1' AS version, SPLIT(token_amounts) AS token_amounts_list, contract_address, transaction_hash, block_timestamp FROM `footprint-etl.ethereum_curve.swap_3coin_v1_event_AddLiquidity`
        UNION ALL SELECT provider, '1' AS version, SPLIT(token_amounts) AS token_amounts_list, contract_address, transaction_hash, block_timestamp FROM `footprint-etl.ethereum_curve.swap_4coin_v1_event_AddLiquidity`
        UNION ALL SELECT provider, '2' AS version, SPLIT(token_amounts) AS token_amounts_list, contract_address, transaction_hash, block_timestamp FROM `footprint-etl.ethereum_curve.swap_2coin_v2_event_AddLiquidity`
        UNION ALL SELECT provider, '2' AS version, SPLIT(token_amounts) AS token_amounts_list, contract_address, transaction_hash, block_timestamp FROM `footprint-etl.ethereum_curve.swap_3coin_v2_event_AddLiquidity`
    ), UNNEST(token_amounts_list) AS token_amount WITH OFFSET AS token_amount_index
    LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool
    ON pool.pool_id = contract_address
) event
LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` token
ON token.contract_address = event.token_address
LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` transaction
ON transaction.hash = event.transaction_hash
WHERE DATE(transaction.block_timestamp) {match_date_filter}
