SELECT  'dForce'                     AS project,
        '1'                          AS version,
        23                           AS protocol_id,
        'lending'                    AS type,
        redeem.block_number,
        redeem.block_timestamp as block_time,
        redeem.transaction_hash as tx_hash,
        redeem.log_index,
        redeem.contract_address,
        redeem.from                  AS operator,
        token.token_address          AS token_address,
        CAST(token.value AS numeric) AS token_amount_raw,
        '' as pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iToken_event_Redeem`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iMSD_event_Redeem`
) AS redeem
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.token_transfers` AS token
ON redeem.from = token.to_address
WHERE redeem.transaction_hash = token.transaction_hash
AND DATE (token.block_timestamp) {match_date_filter}
