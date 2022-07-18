
            WITH source_table AS (
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
where Date(block_timestamp) = '2021-12-15'
            
            ),
            -- 连 tokoen 表
            transactions_token AS(
                SELECT
                s.type,
                s.project,
                s.version,
                s.protocol_id,
                d.protocol_slug,
                s.block_number,
                s.block_timestamp as block_time,
                s.transaction_hash as tx_hash,
                s.log_index,
                s.contract_address,
                s.operator,
                t.symbol as token_symbol,
                s.asset_address as token_address,
                asset_amount / POW(10, t.decimals) AS token_amount,
                asset_amount AS token_amount_raw,
                d.chain AS chain,
                s.pool_id
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON LOWER(s.asset_address) = LOWER(t.contract_address)
                left join `xed-project-237404.footprint.defi_protocol_info` d
                on s.protocol_id = d.protocol_id
            )
            SELECT * FROM transactions_token
            