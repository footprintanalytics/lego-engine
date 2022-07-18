SELECT  'Compound'                     AS project,
        '2'                            AS version,
        10                             AS protocol_id,
        'lending'                      AS type,
        mint.block_number,
        mint.block_timestamp as block_time,
        mint.transaction_hash as tx_hash,
        mint.log_index,
        mint.contract_address,
        LOWER(mint.minter)                    AS operator,
        LOWER(pool.stake_underlying_token[OFFSET(0)]) AS token_address,
        CAST(mintAmount AS numeric)    AS token_amount_raw,
       mint.contract_address as pool_id
FROM `footprint-etl.ethereum_compound.cToken_event_Mint` AS mint
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` AS pool
ON pool.pool_id = mint.contract_address
WHERE DATE (mint.block_timestamp) {match_date_filter}
