SELECT
    user,
    contract_address as pool_address,
    DATE(block_timestamp) AS date,
    "{chain}" as chain
FROM
    `gaia-contract-events.izumi_{chain_lower}.parse_deposit_event`
GROUP BY
    user,
    contract_address,
    DATE(block_timestamp)