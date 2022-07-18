SELECT
    'Pendle' AS project,
    b.version AS version,
    320 AS protocol_id,
    'reward' AS type,
    a.block_number AS block_number,
    a.block_timestamp AS block_timestamp,
    a.transaction_hash AS transaction_hash,
    a.log_index AS log_index,
    a.contract_address AS contract_address,
    a.user AS operator,
    '0x808507121b80c02388fad14726482e061b8da827' AS asset_address,
    CAST(amount as BIGNUMERIC) AS  asset_amount,
    a.contract_address AS pool_id
from `footprint-etl.ethereum_pendle.PendleLiquidityMining_event_PendleRewardsSettled` a
LEFT JOIN
    `footprint-etl-internal.ethereum_pendle.pendle_mining` b
ON
    LOWER(a.contract_address) = LOWER(b.pendle_liquidity_mining_address)
where Date(block_timestamp) {match_date_filter}
            