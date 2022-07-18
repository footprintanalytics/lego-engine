SELECT
    'Pendle' AS project,
    '1' AS version,
    320 AS protocol_id,
    'withdraw' AS type,
    a.block_number AS block_number,
    a.block_timestamp AS block_timestamp,
    a.transaction_hash AS transaction_hash,
    a.log_index AS log_index,
    a.contract_address AS contract_address,
    a.user AS operator,
    a.yieldBearingAsset AS asset_address,
    CAST(ett.value as BIGNUMERIC) AS asset_amount,
    ett.from_address AS pool_id
from (
   select myt.block_number, myt.block_timestamp, myt.transaction_hash, myt.log_index, myt.contract_address, myt.user, nyc.yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleCompoundForge_event_RedeemYieldToken` myt
    left join (
        select distinct contract_address, yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleCompoundForge_event_NewYieldContracts`
    ) nyc
    on
        myt.contract_address = nyc.contract_address
    union all
    select myt.block_number, myt.block_timestamp, myt.transaction_hash, myt.log_index, myt.contract_address, myt.user, nyc.yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleCompoundV2Forge_event_RedeemYieldToken` myt
    left join (
        select distinct contract_address, yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleCompoundV2Forge_event_NewYieldContracts`
    ) nyc
    on
        myt.contract_address = nyc.contract_address
    union all
    select myt.block_number, myt.block_timestamp, myt.transaction_hash, myt.log_index, myt.contract_address, myt.user, nyc.yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleAaveV2Forge_event_RedeemYieldToken` myt
    left join (
        select distinct contract_address, yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleAaveV2Forge_event_NewYieldContracts`
    ) nyc
    on
        myt.contract_address = nyc.contract_address
    union all
    select myt.block_number, myt.block_timestamp, myt.transaction_hash, myt.log_index, myt.contract_address, myt.user, nyc.yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleSushiswapComplexForge_event_RedeemYieldToken` myt
    left join (
        select distinct contract_address, yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleSushiswapComplexForge_event_NewYieldContracts`
    ) nyc
    on
        myt.contract_address = nyc.contract_address
    union all
    select myt.block_number, myt.block_timestamp, myt.transaction_hash, myt.log_index, myt.contract_address, myt.user, nyc.yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleSushiswapSimpleForge_event_RedeemYieldToken` myt
    left join (
        select distinct contract_address, yieldBearingAsset from `footprint-etl.ethereum_pendle.PendleSushiswapSimpleForge_event_NewYieldContracts`
    ) nyc
    on
        myt.contract_address = nyc.contract_address
) a
left join (
    select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where date(block_timestamp) {match_date_filter}
   )ett
on
    a.transaction_hash = ett.transaction_hash
and
    a.user = ett.to_address
and
    lower(ett.token_address) = LOWER(a.yieldBearingAsset)
where Date(a.block_timestamp) {match_date_filter}

union all

SELECT
    'Pendle' AS project,
    '1' AS version,
    320 AS protocol_id,
    'withdraw' AS type,
    mvs.block_number AS block_number,
    mvs.block_timestamp AS block_timestamp,
    mvs.transaction_hash AS transaction_hash,
    mvs.log_index AS log_index,
    mvs.contract_address AS contract_address,
    mvs.user AS operator,
    pm.stake_token AS asset_address,
    CAST(mvs.amount as BIGNUMERIC) AS asset_amount,
    mvs.contract_address AS pool_id
from `footprint-etl.ethereum_pendle.PendleLiquidityMining_event_Withdrawn` mvs
left join
    `footprint-etl-internal.ethereum_pendle.pendle_mining` pm
on
    lower(mvs.contract_address) = lower(pm.pendle_liquidity_mining_address)
where Date(block_timestamp) {match_date_filter}