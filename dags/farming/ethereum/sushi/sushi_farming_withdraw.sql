SELECT  'Sushiswap'                      AS project,
        withdraw.version                 AS version,
        16                               AS protocol_id,
        'withdraw'                       AS type,
        withdraw.block_number,
        withdraw.block_timestamp,
        withdraw.transaction_hash,
        withdraw.log_index,
        withdraw.contract_address,
        withdraw.user                    AS operator,
        lp.lpToken                       AS asset_address,
        CAST(withdraw.amount AS FLOAT64) AS asset_amount,
        lp.lpToken                       AS pool_id
FROM
(
    SELECT  '1' AS version, block_number, block_timestamp, transaction_hash, log_index, contract_address, user, amount, pid
    FROM `footprint-etl.ethereum_sushi.MasterChef_event_Withdraw`
    UNION ALL
    SELECT  '1' AS version, block_number, block_timestamp, transaction_hash, log_index, contract_address, user, amount, pid
    FROM `footprint-etl.ethereum_sushi.MasterChef_event_EmergencyWithdraw`
    UNION ALL
    SELECT  '2' AS version, block_number, block_timestamp, transaction_hash, log_index, contract_address, user, amount, pid
    FROM `footprint-etl.ethereum_sushi.MasterChefV2_event_Withdraw`
    UNION ALL
    SELECT  '2' AS version, block_number, block_timestamp, transaction_hash, log_index, contract_address, user, amount, pid
    FROM `footprint-etl.ethereum_sushi.MasterChefV2_event_EmergencyWithdraw`
) withdraw
LEFT JOIN `footprint-etl.ethereum_sushi.pid_lp_token` lp
ON lp.version = withdraw.version AND lp.pid = withdraw.pid
-- TODO withdraw.amount = 0
WHERE withdraw.amount != '0' AND DATE(withdraw.block_timestamp) {match_date_filter}
