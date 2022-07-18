    SELECT
        'Compound' AS project,
        '2' AS version,
        10 AS protocol_id,
        'deposit' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        events.contract_address,
        minter AS borrower,
        c.stake_underlying_token[OFFSET(0)] AS asset_address,
        CAST(mintAmount AS BIGNUMERIC) AS asset_amount,
        events.contract_address as pool_id
    FROM (
        SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC2_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Mint`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Mint`
    ) events
    LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` c ON c.pool_id = events.contract_address
    WHERE Date(block_timestamp) {match_date_filter}

    UNION ALL
    -- Compound remove collateral
    SELECT
        'Compound' AS project,
        '2' AS version,
        10 AS protocol_id,
        'withdraw' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        events.contract_address,
        redeemer AS borrower,
        c.stake_underlying_token[OFFSET(0)] AS asset_address,
        CAST(redeemAmount AS BIGNUMERIC) AS asset_amount,
        events.contract_address as pool_id
    FROM (
        SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC2_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Redeem`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Redeem`
    ) events
    LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` c ON c.pool_id = events.contract_address
    WHERE Date(block_timestamp) {match_date_filter}

     UNION ALL
    -- Compound remove collateral
    SELECT
        'Compound' AS project,
        '2' AS version,
        10 AS protocol_id,
        'liquidation' AS type,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        events.contract_address,
        borrower,
        cTokenCollateral AS asset_address,
        CAST(seizeTokens AS BIGNUMERIC) AS asset_amount,
        events.contract_address as pool_id
    FROM (
        SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC2_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_LiquidateBorrow`
        UNION ALL
        SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_LiquidateBorrow`
    ) events
    WHERE Date(block_timestamp) {match_date_filter}