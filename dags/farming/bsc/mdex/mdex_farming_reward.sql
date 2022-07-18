SELECT  'MDEX'                   AS project,
        '1'                      AS version,
        254                      AS protocol_id,
        'reward'                 AS type,
        w.block_number,
        w.block_timestamp,
        w.transaction_hash,
        w.log_index,
        w.contract_address,
        w.user                   AS operator,
        t.token_address          AS asset_address,
        CAST(t.value AS FLOAT64) AS asset_amount,
        w.pool_id                AS pool_id
FROM
(
    SELECT _w.block_number,
           _w.block_timestamp,
           _w.transaction_hash,
           _w.log_index,
           _w.contract_address,
           _w.user,
           _w.amount,
           _t.lpToken AS pool_id
    FROM `footprint-etl.bsc_mdex.BSCPool_event_Withdraw` _w
    LEFT JOIN `footprint-etl.bsc_mdex.pid_lp_token` _t
    ON _w.pid = _t.pid

    UNION ALL

    SELECT _w.block_number,
           _w.block_timestamp,
           _w.transaction_hash,
           _w.log_index,
           _w.contract_address,
           _w.user,
           _w.amount,
           _w.contract_address AS pool_id
    FROM `footprint-etl.bsc_mdex.BoardRoom_event_Withdraw` _w
) w
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.token_transfers` t
ON
(
    t.transaction_hash = w.transaction_hash
    AND t.from_address = w.contract_address
    AND t.to_address = w.user
    -- 过滤提现
    AND t.value != w.amount
    AND DATE(t.block_timestamp) {match_date_filter}
)
WHERE
(
    w.amount != '0'
    AND t.value != '0'
    AND DATE(w.block_timestamp) {match_date_filter}
)
