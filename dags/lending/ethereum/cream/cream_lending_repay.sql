SELECT  'Cream'                               AS project,
        '1'                                   AS version,
        18                                    AS protocol_id,
        'lending'                             AS type,
        repay.block_number,
        repay.block_timestamp as block_time,
        repay.transaction_hash as tx_hash,
        repay.log_index,
        repay.contract_address,
        LOWER(repay.borrower)                 AS operator,
        pool.stake_token[OFFSET(0)]           AS token_address,
        CAST(repay.repayAmount AS BIGNUMERIC) AS token_amount_raw,
        ''                AS pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_eth_event_RepayBorrow`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_iron_bank_event_RepayBorrow`
) AS repay
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = repay.contract_address
WHERE Date(repay.block_timestamp) {match_date_filter}
