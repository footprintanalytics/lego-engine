SELECT  'MDEX'                    AS project,
        '1'                       AS version,
        254                       AS protocol_id,
        'deposit'                 AS type,
        d.block_number,
        d.block_timestamp,
        d.transaction_hash,
        d.log_index,
        d.contract_address,
        d.user                    AS operator,
        t.token_address           AS asset_address,
        CAST(d.amount AS FLOAT64) AS asset_amount,
        d.pool_id                 AS pool_id
FROM
(
    SELECT _d.block_number,
           _d.block_timestamp,
           _d.transaction_hash,
           _d.log_index,
           _d.contract_address,
           _d.user,
           _d.amount,
           _t.lpToken AS pool_id
    FROM `footprint-etl.bsc_mdex.BSCPool_event_Deposit` _d
    LEFT JOIN `footprint-etl.bsc_mdex.pid_lp_token` _t
    ON _d.pid = _t.pid

    UNION ALL

    SELECT _d.block_number,
           _d.block_timestamp,
           _d.transaction_hash,
           _d.log_index,
           _d.contract_address,
           _d.user,
           _d.amount,
           _d.contract_address AS pool_id
    FROM `footprint-etl.bsc_mdex.BoardRoom_event_Deposit` _d
) d
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.token_transfers` t
ON
(
    t.transaction_hash = d.transaction_hash
    AND t.from_address = d.user
    AND t.to_address = d.contract_address
    AND t.value = d.amount
    AND DATE(t.block_timestamp) {match_date_filter}
)
WHERE
(
    d.amount != '0'
    AND DATE(d.block_timestamp) {match_date_filter}
)
