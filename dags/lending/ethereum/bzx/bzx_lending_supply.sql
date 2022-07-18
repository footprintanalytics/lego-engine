
SELECT * from (
    SELECT
        'bzx' AS project,
        '1' AS version,
        103 AS protocol_id,
        'lending' AS type,
        block_number,
        block_timestamp as block_time,
        transaction_hash as tx_hash,
        log_index,
        contract_address,
        user AS operator,
        depositToken AS token_address,
        cast(depositAmount as float64) AS token_amount_raw,
       '' as pool_id
    from `footprint-etl.ethereum_bzx.LoanMaintenance_event_DepositCollateral`
    union all
    SELECT
        'bzx' AS project,
        '1' AS version,
        103 AS protocol_id,
        'lending' AS type,
        block_number,
        block_timestamp as block_time,
        transaction_hash as tx_hash,
        log_index,
        contract_address,
        user AS operator,
        collateralToken AS token_address,
        cast(newCollateral as float64) AS token_amount_raw,
       '' as pool_id
    from `footprint-etl.ethereum_bzx.LoanOpenings_event_Borrow`
)
            