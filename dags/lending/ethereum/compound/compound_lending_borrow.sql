SELECT  'Compound'                     AS project,
        '2'                            AS version,
        10                             AS protocol_id,
        'lending'                      AS type,
        borrow.block_number,
        borrow.block_timestamp as block_time,
        borrow.transaction_hash as tx_hash,
        borrow.log_index,
        borrow.contract_address,
        LOWER(borrow.borrower) as operator,
        LOWER(pool.stake_underlying_token[OFFSET(0)]) AS token_address,
        CAST(borrowAmount AS numeric)  AS token_amount_raw,
        borrow.contract_address as pool_id
FROM `footprint-etl.ethereum_compound.cToken_event_Borrow` AS borrow
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = borrow.contract_address
WHERE DATE (borrow.block_timestamp) {match_date_filter}
