SELECT  'Cream'                                 AS project,
        '1'                                     AS version,
        18                                      AS protocol_id,
        'lending'                               AS type,
        borrow.block_number,
        borrow.block_timestamp as block_time,
        borrow.transaction_hash as tx_hash,
        borrow.log_index,
        borrow.contract_address,
        LOWER(borrow.borrower)                  AS operator,
        pool.stake_token[OFFSET(0)]             AS token_address,
        CAST(borrow.borrowAmount AS BIGNUMERIC) AS token_amount_raw,
        borrow.contract_address                 AS pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_eth_event_Borrow`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_iron_bank_event_Borrow`
) AS borrow
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = borrow.contract_address
WHERE Date(borrow.block_timestamp) {match_date_filter}
