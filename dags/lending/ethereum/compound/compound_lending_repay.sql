SELECT  'Compound'                     AS project,
        '2'                            AS version,
        10                             AS protocol_id,
        'lending'                      AS type,
        repay.block_number,
        repay.block_timestamp as block_time,
        repay.transaction_hash as tx_hash,
        repay.log_index,
        repay.contract_address,
        LOWER(repay.borrower) AS operator,
        LOWER(pool.stake_underlying_token[OFFSET(0)]) AS token_address,
        CAST(repayAmount AS numeric)   AS token_amount_raw,
       repay.contract_address as pool_id
FROM `footprint-etl.ethereum_compound.cToken_event_RepayBorrow` AS repay
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = repay.contract_address
WHERE DATE (repay.block_timestamp) {match_date_filter}
