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
        borrower,
        CASE
            WHEN _collateral= '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _collateral
        END  AS token_collateral_address,
         CAST(_liquidatedCollateralAmount AS BIGNUMERIC) AS token_collateral_amount_raw,
         _liquidator AS liquidator,
        CASE
            WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
            ELSE _reserve
        END AS repay_token_address,
        CAST(_purchaseAmount AS BIGNUMERIC) AS repay_token_amount_raw,
       '' as pool_id
    FROM (
        SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, _reserve, _collateral, _liquidator , _liquidatedCollateralAmount,_purchaseAmount , _user AS borrower
        FROM `footprint-etl.ethereum_aave.LendingPool_event_LiquidationCall`

        UNION ALL

        SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, _reserve, _collateral, _liquidator, _liquidatedCollateralAmount, _purchaseAmount, _user AS borrower
        FROM `footprint-etl.ethereum_aave.LendingPoolOld_event_LiquidationCall`

        UNION ALL

        SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, _reserve, _collateral, _liquidator, _liquidatedCollateralAmount, _purchaseAmount, _user AS borrower
        FROM `footprint-etl.ethereum_aave.LendingPool1_event_LiquidationCall`
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
        borrower,
        collateralAsset AS token_collateral_address,
        CAST(liquidatedCollateralAmount AS BIGNUMERIC) AS token_collateral_amount_raw,
        liquidator,
        debtAsset AS repay_token_address,
        CAST(debtToCover AS BIGNUMERIC)  AS repay_token_amount_raw,
       '' as pool_id
    FROM (
         SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, debtAsset,debtToCover,collateralAsset, liquidatedCollateralAmount ,liquidator, user AS borrower
         FROM `footprint-etl.ethereum_aave.LendingPool_v2_event_LiquidationCall`
       )
       WHERE Date(block_timestamp) {match_date_filter}