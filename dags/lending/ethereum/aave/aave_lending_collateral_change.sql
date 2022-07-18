SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        CAST(_amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool_event_Deposit`
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL
SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        CAST(_amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool1_event_Deposit`
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL
SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        CAST(_amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPoolOld_event_Deposit`
    WHERE Date(block_timestamp) {match_date_filter}


    UNION ALL

    -- Aave remove collateral
    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        -CAST(_amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool_event_RedeemUnderlying`
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL

    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        -CAST(_amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool1_event_RedeemUnderlying`
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL

    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS asset_address,
        -CAST(_amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPoolOld_event_RedeemUnderlying`
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL

    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'liquidation' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        _collateral AS asset_address,
        -CAST(_liquidatedCollateralAmount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool_event_LiquidationCall`
    WHERE _receiveAToken = 'false'
    AND Date(block_timestamp) {match_date_filter}
    
    UNION ALL

    SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'liquidation' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        _collateral AS asset_address,
        -CAST(_liquidatedCollateralAmount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool1_event_LiquidationCall`
    WHERE _receiveAToken = 'false'
    AND Date(block_timestamp) {match_date_filter}
    UNION ALL
     SELECT
        'Aave' AS project,
        '1' AS version,
        6 AS protocol_id,
        'liquidation' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        _user AS borrower,
        _collateral AS asset_address,
        -CAST(_liquidatedCollateralAmount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPoolOld_event_LiquidationCall`
    WHERE _receiveAToken = 'false'
    AND Date(block_timestamp) {match_date_filter}


    UNION ALL

    -- Aave 2 add collateral
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        user AS borrower,
        reserve AS asset_address,
        CAST(amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool_v2_event_Deposit`
    WHERE Date(block_timestamp) {match_date_filter}


    UNION ALL
    -- Aave 2 remove collateral
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        user AS borrower,
        reserve AS asset_address,
        -CAST(amount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool_v2_event_Withdraw`
    WHERE Date(block_timestamp) {match_date_filter}


    UNION ALL
    --Aave 2 liquidation calls
    SELECT
        'Aave' AS project,
        '2' AS version,
        6 AS protocol_id,
        'liquidation' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        user AS borrower,
        collateralAsset AS asset_address,
        -CAST(liquidatedCollateralAmount AS BIGNUMERIC) AS asset_amount,
       '' as pool_id
    FROM `footprint-etl.ethereum_aave.LendingPool_v2_event_LiquidationCall`
    WHERE receiveAToken = 'false'
    AND Date(block_timestamp) {match_date_filter}