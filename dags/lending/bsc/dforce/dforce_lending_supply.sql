SELECT  'dForce'                     AS project,
        '1'                          AS version,
        23                           AS protocol_id,
        'lending'                    AS type,
        mint.block_number,
        mint.block_timestamp as block_time,
        mint.transaction_hash as tx_hash,
        mint.log_index,
        mint.contract_address,
        mint.sender                  AS operator,
        token.token_address          AS token_address,
        CAST(token.value AS numeric) AS token_amount_raw,
        '' as pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iToken_event_Mint`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iMSD_event_Mint`
) AS mint
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.token_transfers` AS token
ON mint.sender = token.from_address
WHERE mint.transaction_hash = token.transaction_hash
AND DATE (token.block_timestamp) {match_date_filter}
