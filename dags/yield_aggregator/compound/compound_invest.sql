SELECT
    transaction_hash,
    block_timestamp,
    minter AS op_user,
    events.contract_address,
    0 AS gas,
    0 AS gas_price,
    'None' AS from_address,
    'None' AS to_address,
    c.underlying_token_address AS token_address,
    CAST(mintAmount AS FLOAT64) AS value,
    'deposit' AS operation
FROM (
--     SELECT * FROM `blockchain-etl.ethereum_compound.cAAVE_event_Mint`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cLINK_event_Mint`
    UNION ALL
--     SELECT * FROM `blockchain-etl.ethereum_compound.cMKR_event_Mint`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Mint`
    UNION ALL
--     SELECT * FROM `blockchain-etl.ethereum_compound.cSUSHI_event_Mint`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Mint`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC2_event_Mint`
    UNION ALL
--     SELECT * FROM `blockchain-etl.ethereum_compound.cYFI_event_Mint`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Mint`
) events
LEFT JOIN `xed-project-237404.footprint_etl.compound_view_ctokens` c ON events.contract_address = c.contract_address
WHERE Date(block_timestamp) {match_date_filter}

UNION ALL
-- Compound remove collateral
SELECT
    transaction_hash,
    block_timestamp,
    redeemer AS op_user,
    events.contract_address,
    0 AS gas,
    0 AS gas_price,
    'None' AS from_address,
    'None' AS to_address,
    c.underlying_token_address AS token_address,
    CAST(redeemAmount AS FLOAT64) AS value,
    'withdraw' AS operation
FROM (
--     SELECT * FROM `blockchain-etl.ethereum_compound.cAAVE_event_Redeem`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cBAT_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cCOMP_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cDAI_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cETH_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cLINK_event_Redeem`
    UNION ALL
--     SELECT * FROM `blockchain-etl.ethereum_compound.cMKR_event_Redeem`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cREP_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cSAI_event_Redeem`
    UNION ALL
--     SELECT * FROM `blockchain-etl.ethereum_compound.cSUSHI_event_Redeem`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cTUSD_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cUNI_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cUSDC_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cUSDT_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC_event_Redeem`
    UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cWBTC2_event_Redeem`
    UNION ALL
--     SELECT * FROM `blockchain-etl.ethereum_compound.cYFI_event_Redeem`
--     UNION ALL
    SELECT * FROM `blockchain-etl.ethereum_compound.cZRX_event_Redeem`
) events
LEFT JOIN `xed-project-237404.footprint_etl.compound_view_ctokens` c ON events.contract_address = c.contract_address
WHERE Date(block_timestamp) {match_date_filter}