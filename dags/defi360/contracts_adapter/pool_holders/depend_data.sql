SELECT
    user,
    contract_address as pool_address,
    DATE(block_timestamp) AS date,
    "{chain}" as chain
FROM
    `footprint-etl.{chain}_izumi_autoparse.Deposit_event_addressIndexed_uint256_uint256`
GROUP BY
    user,
    contract_address,
    DATE(block_timestamp)