SELECT  'Cream'                                 AS project,
        '1'                                     AS version,
        18                                      AS protocol_id,
        'lending'                               AS type,
        redeem.block_number,
        redeem.block_timestamp as block_time,
        redeem.transaction_hash as tx_hash,
        redeem.log_index,
        redeem.contract_address,
        LOWER(redeem.redeemer)                  AS operator,
        pool.stake_token[OFFSET(0)]             AS token_address,
        CAST(redeem.redeemAmount AS BIGNUMERIC) AS token_amount_raw,
        ''                AS pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_eth_event_Redeem`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_iron_bank_event_Redeem`
) AS redeem
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = redeem.contract_address
WHERE Date(redeem.block_timestamp) {match_date_filter}
