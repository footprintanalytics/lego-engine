SELECT  'dForce'                     AS project,
        '1'                          AS version,
        23                           AS protocol_id,
        'lending'                    AS type,
        repay.block_number,
        repay.block_timestamp as block_time,
        repay.transaction_hash as tx_hash,
        repay.log_index,
        repay.contract_address,
        repay.borrower as operator,
        token.token_address          AS token_address,
        CAST(token.value AS numeric) AS token_amount_raw,
        '' as pool_id
FROM
(
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iToken_event_RepayBorrow`
    UNION ALL
    SELECT  *
    FROM `footprint-etl.bsc_dforce.iMSD_event_RepayBorrow`
) AS repay
LEFT JOIN `footprint-blockchain-etl.crypto_bsc.token_transfers` AS token
ON repay.borrower = token.from_address
WHERE repay.transaction_hash = token.transaction_hash
AND DATE (token.block_timestamp) {match_date_filter}
