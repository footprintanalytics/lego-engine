WITH collateral AS (
    SELECT * FROM
    `footprint-etl.ethereum_lending_{project}.{project}_lending_supply_all`
    UNION ALL
    SELECT * FROM
    `footprint-etl.ethereum_lending_{project}.{project}_lending_withdraw_all`
)

SELECT *
FROM
(
    SELECT
    Date(block_time) AS date,
    project,
    version,
    'borrow' AS type,
    COUNT(project) AS nums,
    SUM(token_amount) AS token_amount
    FROM
    `footprint-etl.ethereum_lending_{project}.{project}_lending_borrow_all`
    WHERE Date(block_time) >= '2021-10-01' AND Date(block_time) < '2021-11-01'
    GROUP BY 1, 2, 3
)
UNION ALL
(
    SELECT
    Date(block_time) AS date,
    project,
    version,
    'repay' AS type,
    COUNT(project) AS nums,
    SUM(token_amount) AS token_amount
    FROM
    `footprint-etl.ethereum_lending_{project}.{project}_lending_repay_all`
    WHERE Date(block_time) >= '2021-10-01' AND Date(block_time) < '2021-11-01'
    GROUP BY 1, 2, 3
)
UNION ALL
(
    SELECT
    Date(block_time) AS date,
    project,
    version,
    'collateral' AS type,
    COUNT(project) AS nums,
    SUM(token_amount) AS token_amount
    FROM collateral
    WHERE Date(block_time) >= '2021-10-01' AND Date(block_time) < '2021-11-01'
    GROUP BY 1, 2, 3
)
ORDER BY 1, 2