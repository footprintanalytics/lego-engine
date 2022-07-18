-- Aave add collateral
SELECT
    'deposit' AS operation,
    contract_address,
    block_timestamp,
    transaction_hash,
    _user AS op_user,
    0 AS gas,
    0 AS gas_price,
    'None' AS from_address,
    'None' AS to_address,
    CAST(_amount AS FLOAT64) AS value,
    CASE
        WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
        ELSE _reserve
    END AS token_address,
FROM `blockchain-etl.ethereum_aave.LendingPool_event_Deposit`
WHERE Date(block_timestamp) {match_date_filter}

UNION ALL

-- Aave remove collateral
SELECT
    'withdraw' AS operation,
    contract_address,
    block_timestamp,
    transaction_hash,
    _user AS op_user,
    0 AS gas,
    0 AS gas_price,
    'None' AS from_address,
    'None' AS to_address,
    CAST(_amount AS FLOAT64) AS value,
    CASE
        WHEN _reserve = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' --Use WETH instead of Aave "mock" address
        ELSE _reserve
    END AS token_address
FROM `blockchain-etl.ethereum_aave.LendingPool_event_RedeemUnderlying`
WHERE Date(block_timestamp) {match_date_filter}

UNION ALL

-- Aave 2 add collateral
SELECT
    'deposit' AS operation,
    contract_address,
    block_timestamp,
    transaction_hash,
    user AS op_user,
    0 AS gas,
    0 AS gas_price,
    'None' AS from_address,
    'None' AS to_address,
    CAST(amount AS FLOAT64) AS value,
    reserve AS token_address
FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_Deposit`
WHERE Date(block_timestamp) {match_date_filter}

UNION ALL

-- Aave 2 remove collateral
SELECT
    'withdraw' AS operation,
    contract_address,
    block_timestamp,
    transaction_hash,
    user AS op_user,
    0 AS gas,
    0 AS gas_price,
    'None' AS from_address,
    'None' AS to_address,
    CAST(amount AS FLOAT64) AS value,
    reserve AS token_address
FROM `blockchain-etl.ethereum_aave.LendingPool_v2_event_Withdraw`
WHERE Date(block_timestamp) {match_date_filter}