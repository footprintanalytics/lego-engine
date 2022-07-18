SELECT 'Pancakeswap' AS project,
       '1'           AS version,
       100           AS protocol_id,
       'Farming'     AS type,
       *
from (
--     LP 的
         SELECT deposit.block_number               as block_number,
                deposit.block_timestamp            as block_timestamp,
                deposit.transaction_hash           as transaction_hash,
                deposit.log_index                  as log_index,
                deposit.contract_address           as contract_address,
                deposit.user                       AS operator,
                pair.pair                          AS asset_address,
                CAST(deposit.amount as BIGNUMERIC) AS asset_amount,
                pair.pair                          as pool_id
         from (select *
               from `footprint-etl.bsc_pancakeswap.MasterChef_event_Deposit`
               where DATE (block_timestamp) {match_date_filter}) deposit
         left join `footprint-blockchain-etl.crypto_bsc.token_transfers` tt
                   on tt.from_address = deposit.user and tt.value = deposit.amount and
                      tt.transaction_hash = deposit.transaction_hash
                       and tt.to_address = deposit.contract_address and DATE (tt.block_timestamp) {match_date_filter}
    left join `footprint-etl.bsc_pancakeswap.UniswapV2Pair_event_PairCreated` pair
on pair.pair = tt.token_address
--                 普通 单币的pool,都是质押cake，挖其他小币
UNION ALL
SELECT block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       user                                         AS operator,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS asset_address,
       CAST(amount as BIGNUMERIC)                   AS asset_amount,
       contract_address                             AS pool_id
FROM `footprint-etl.bsc_pancakeswap.SmartChef_event_Deposit`
where  DATE (block_timestamp) {match_date_filter}

UNION ALL
--  AutoCake 自动复投池
SELECT block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       sender                                       as operator,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS asset_address,
       CAST(amount as BIGNUMERIC)                   AS asset_amount,
       contract_address                             AS pool_id
FROM `footprint-etl.bsc_pancakeswap.CakeVault_event_Deposit`
where DATE (block_timestamp) {match_date_filter}

UNION ALL
-- 手动cake池
SELECT block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       user                                         as operator,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS asset_address,
       CAST(amount as BIGNUMERIC)                   AS asset_amount,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS pool_id
FROM `footprint-etl.bsc_pancakeswap.MasterChef_event_Deposit`
WHERE CAST(pid AS INT64) <> 0)
where asset_address is not null and DATE (block_timestamp) {match_date_filter}
            