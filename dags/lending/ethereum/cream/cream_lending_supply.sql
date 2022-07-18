SELECT  'Cream'                             AS project,
        '1'                                 AS version,
        18                                  AS protocol_id,
        'lending'                           AS type,
        mint.block_number,
        mint.block_timestamp as block_time,
        mint.transaction_hash as tx_hash,
        mint.log_index,
        mint.contract_address,
        LOWER(mint.minter)                  AS operator,
        pool.stake_token[OFFSET(0)]         AS token_address,
        CAST(mint.mintAmount AS BIGNUMERIC) AS token_amount_raw,
        ''             AS pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_eth_event_Mint`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.ethereum_cream.cream_iron_bank_event_Mint`
) AS mint
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = mint.contract_address
WHERE Date(mint.block_timestamp) {match_date_filter}
