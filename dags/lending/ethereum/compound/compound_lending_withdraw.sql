SELECT  'Compound'                     AS project,
        '2'                            AS version,
        10                             AS protocol_id,
        'lending'                      AS type,
        redeem.block_number,
        redeem.block_timestamp as block_time,
        redeem.transaction_hash as tx_hash,
        redeem.log_index,
        redeem.contract_address,
        LOWER(redeem.redeemer)                AS operator,
        LOWER(pool.stake_underlying_token[OFFSET(0)]) AS token_address,
        CAST(redeemAmount AS numeric)  AS token_amount_raw,
        redeem.contract_address as pool_id
FROM `footprint-etl.ethereum_compound.cToken_event_Redeem` AS redeem
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = redeem.contract_address
WHERE DATE (redeem.block_timestamp) {match_date_filter}
