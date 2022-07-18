SELECT  'dForce'                     AS project,
        '1'                          AS version,
        23                           AS protocol_id,
        'lending'                    AS type,
        borrow.block_number,
        borrow.block_timestamp as block_time,
        borrow.transaction_hash as tx_hash,
        borrow.log_index,
        borrow.contract_address,
        borrow.borrower as operator,
        token.token_address          AS token_address,
        CAST(token.value AS numeric) AS token_amount_raw,
        '' as pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iToken_event_Borrow`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iMSD_event_Borrow`
) AS borrow
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.token_transfers` AS token
ON borrow.borrower = token.to_address
WHERE borrow.transaction_hash = token.transaction_hash
AND DATE (token.block_timestamp) {match_date_filter}
