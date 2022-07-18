-- token0
SELECT  'Mdex'                                          AS project,
        e.sender                                        AS liquidity_provider,
        '1'                                             AS version,
        254                                             AS protocol_id,
        t.symbol                                        AS token_symbol,
        CAST(e.amount0 AS float64) / POW(10,t.decimals) AS token_amount,
        (CAST (NULL AS float64))                        AS usd_value_of_token,
        CAST(e.amount0 AS float64)                      AS token_amount_raw,
        'AMM'                                           AS type,
        token0                                          AS token_address,
        e.contract_address                              AS exchange_address,
        e.transaction_hash                              AS tx_hash,
        DATETIME(e.block_timestamp)                     AS block_time,
        tx.from_address                                 AS tx_from
FROM `footprint-etl.bsc_mdex.MdexPair_event_Mint` e
LEFT JOIN `footprint-etl.bsc_mdex.MdexFactory_event_PairCreated` p
ON (e.contract_address = p.pair)
LEFT JOIN `xed-project-237404.footprint_etl.bsc_erc20_tokens` t
ON t.contract_address = p.token0
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.transactions` tx
ON tx.HASH = e.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}
WHERE DATE(e.block_timestamp) {match_date_filter}
UNION ALL
-- token1
SELECT  'Mdex'                                          AS project,
        e.sender                                        AS liquidity_provider,
        '1'                                             AS version,
        254                                             AS protocol_id,
        t.symbol                                        AS token_symbol,
        CAST(e.amount1 AS float64) / POW(10,t.decimals) AS token_amount,
        (CAST (NULL AS float64))                        AS usd_value_of_token,
        CAST(e.amount1 AS float64)                      AS token_amount_raw,
        'AMM'                                           AS type,
        token1                                          AS token_address,
        e.contract_address                              AS exchange_address,
        e.transaction_hash                              AS tx_hash,
        DATETIME(e.block_timestamp)                     AS block_time,
        tx.from_address                                 AS tx_from
FROM `footprint-etl.bsc_mdex.MdexPair_event_Mint` e
LEFT JOIN `footprint-etl.bsc_mdex.MdexFactory_event_PairCreated` p
ON (e.contract_address = p.pair)
LEFT JOIN `xed-project-237404.footprint_etl.bsc_erc20_tokens` t
ON t.contract_address = p.token1
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.transactions` tx
ON tx.HASH = e.transaction_hash AND DATE(tx.block_timestamp) {match_date_filter}
WHERE DATE(e.block_timestamp) {match_date_filter}
