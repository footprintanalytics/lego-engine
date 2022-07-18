SELECT  'Sushiswap'                     AS project,
        deposit.version                 AS version,
        16                              AS protocol_id,
        'deposit'                       AS type,
        deposit.block_number,
        deposit.block_timestamp,
        deposit.transaction_hash,
        deposit.log_index,
        deposit.contract_address,
        deposit.user                    AS operator,
        lp.lpToken                      AS asset_address,
        CAST(deposit.amount AS FLOAT64) AS asset_amount,
        lp.lpToken                      AS pool_id
FROM
(
    SELECT  '1' AS version, block_number, block_timestamp, transaction_hash, log_index, contract_address, user, amount, pid
    FROM `footprint-etl.ethereum_sushi.MasterChef_event_Deposit`
    UNION ALL
    SELECT  '2' AS version, block_number, block_timestamp, transaction_hash, log_index, contract_address, user, amount, pid
    FROM `footprint-etl.ethereum_sushi.MasterChefV2_event_Deposit`
) deposit
LEFT JOIN `footprint-etl.ethereum_sushi.pid_lp_token` lp
ON lp.version = deposit.version AND lp.pid = deposit.pid
-- TODO deposit.amount = 0
WHERE deposit.amount != '0' AND DATE(deposit.block_timestamp) {match_date_filter}
