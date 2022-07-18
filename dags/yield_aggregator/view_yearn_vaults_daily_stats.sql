WITH dayly_erc20_transfer AS (
    SELECT
    *
    FROM `bigquery-public-data.crypto_ethereum.token_transfers`
    WHERE Date(block_timestamp) = '2021-06-01'
),

yearn_deposited AS (
    SELECT
    Date(s.block_timestamp) as day,
    s.block_timestamp,
    s.transaction_hash,
    s.token_address as asset_address,
    CAST(curve.value AS FLOAT64) / 1e18 as token_amount
    FROM dayly_erc20_transfer s
    LEFT JOIN dayly_erc20_transfer curve ON s.transaction_hash = curve.transaction_hash
    WHERE
        s.token_address IN (SELECT contract_address FROM `xed-project-237404.footprint_etl.yearn_vaults`)
        -- s.token_address = "0x5dbcf33d8c2e976c6b560249878e6f1491bca25c"
        -- and curve.token_address = '0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8'
        AND s.from_address = '0x0000000000000000000000000000000000000000'
        AND s.to_address = curve.from_address
    -- GROUP BY s.token_address
),

yearn_withdraw AS (
    SELECT
    Date(s.block_timestamp) as day,
    s.block_timestamp,
    s.transaction_hash,
    s.token_address as asset_address,
    CAST(curve.value AS FLOAT64) / 1e18 as token_amount
    FROM dayly_erc20_transfer s
    LEFT JOIN dayly_erc20_transfer curve ON s.transaction_hash = curve.transaction_hash
    WHERE
        s.token_address IN (SELECT contract_address FROM `xed-project-237404.footprint_etl.yearn_vaults`)
        AND s.to_address = '0x0000000000000000000000000000000000000000'
        AND s.from_address = curve.to_address
),

pretty as (
    SELECT
    *, "deposit" as type
    FROM yearn_deposited

    UNION ALL

    SELECT
    *,  "withdraw" as type
    FROM yearn_withdraw
),

daily as (
    SELECT
    day,
    asset_address,
    SUM(1) as trade_count,
    SUM(
        CASE
            WHEN type = 'deposit' THEN token_amount
            ELSE 0
        END
    ) as deposit_amount,
    SUM(
        CASE
            WHEN type = 'withdraw' THEN token_amount
            ELSE 0
        END
    ) as withdraw_amount,
    FROM pretty
    GROUP BY day, asset_address
)


SELECT
p.*,
yv.name as asset_symbol
FROM daily p
LEFT JOIN `xed-project-237404.footprint_etl.yearn_vaults` yv
ON p.asset_address = yv.contract_address

