SELECT 'Pancakeswap' AS project,
       '1'           AS version,
       100           AS protocol_id,
       'Farming'     AS type,
       *
from (
--         LP 的
         SELECT withdraw.block_number               as block_number,
                withdraw.block_timestamp            as block_timestamp,
                withdraw.transaction_hash           as transaction_hash,
                withdraw.log_index                  as log_index,
                withdraw.contract_address           as contract_address,
                withdraw.user                       AS operator,
                pair.pair                           AS asset_address,
                CAST(withdraw.amount as BIGNUMERIC) AS asset_amount,
                pair.pair                           as pool_id
         from (select *
               from `footprint-etl.bsc_pancakeswap.MasterChef_event_Withdraw`
               where DATE (block_timestamp) {match_date_filter}) withdraw
         left join `footprint-blockchain-etl.crypto_bsc.token_transfers` tt
                   on tt.to_address = withdraw.user and tt.value = withdraw.amount and
                      tt.transaction_hash = withdraw.transaction_hash and
                      tt.from_address = withdraw.contract_address and DATE (tt.block_timestamp) {match_date_filter}
    left join `footprint-etl.bsc_pancakeswap.UniswapV2Pair_event_PairCreated` pair
on pair.pair = tt.token_address
Union All
-- 普通单币池
SELECT block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       user                                         as operator,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS asset_address,
       CAST(amount as BIGNUMERIC)                   AS asset_amount,
       contract_address                             AS pool_id
FROM `footprint-etl.bsc_pancakeswap.SmartChef_event_Withdraw`
where DATE (block_timestamp) {match_date_filter}

UNION ALL
-- 手动 cake 池 没有自己的合约地址，直接调用masterChef pool的,所以直接cake地址作为token 和 pool id
SELECT block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       user                                         as operator,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS asset_address,
       CAST(amount as BIGNUMERIC)                   AS asset_amount,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS pool_id
FROM `footprint-etl.bsc_pancakeswap.MasterChef_event_Withdraw`
WHERE pid = '0'
  AND amount <> '0'
  and DATE (block_timestamp) {match_date_filter}

UNION ALL
-- AutoCake 自动复投池 ,合约地址是独立的 ，直接质押 cake的
SELECT block_number,
       block_timestamp,
       transaction_hash,
       log_index,
       contract_address,
       SENDER                                       as operator,
       '0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82' AS asset_address,
       CAST(amount as BIGNUMERIC)                   AS asset_amount,
       contract_address                             AS pool_id
FROM `footprint-etl.bsc_pancakeswap.CakeVault_event_Withdraw` )
where asset_address is not null and DATE (block_timestamp) {match_date_filter}