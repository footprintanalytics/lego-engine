SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'lending' as type,
        block_number,
        block_timestamp as block_time,
        transaction_hash as tx_hash,
        log_index,
        contract_address,
        borrower as operator,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS token_address,
        CAST(_amount AS BIGNUMERIC) AS token_amount_raw,
       '' as pool_id
    FROM (
        SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, _reserve, _amount, _user AS borrower
        FROM `footprint-etl.ethereum_aave.LendingPool_event_RedeemUnderlying`

        UNION ALL

        SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, _reserve, _amount, _user AS borrower
        FROM `footprint-etl.ethereum_aave.LendingPoolOld_event_RedeemUnderlying`

        UNION ALL

        SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, _reserve, _amount, _user AS borrower
        FROM `footprint-etl.ethereum_aave.LendingPool1_event_RedeemUnderlying`
    )
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL

SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'lending' as type,
        block_number,
        block_timestamp as block_time,
        transaction_hash as tx_hash,
        log_index,
        contract_address,
        borrower as operator,
        CASE
            WHEN reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE reserve
        END AS token_address,
        CAST(amount AS BIGNUMERIC) AS token_amount_raw,
       '' as pool_id
    FROM (
         SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, reserve, amount, user AS borrower
         FROM `footprint-etl.ethereum_aave.LendingPool_v2_event_Withdraw`
       )
       WHERE Date(block_timestamp) {match_date_filter}