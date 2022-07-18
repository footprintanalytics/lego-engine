SELECT 'Pancakeswap' AS project,
       '1'           AS version,
       100           AS protocol_id,
       'Farming'     AS type,
       *

from (
--     跟着 withdraw一起拿到的收益 ，在withdraw 的token transfers中会多一条奖励的转账记录：https://bscscan.com/tx/0x3b7b47350697a1236536662c4b7877dffede8ca226c2297ebb904119f98fc2b5
--    todo 存在这用户投在其他defi平台，然后那个平台通过合约投资到pancake的case，这样会有一些异常的数据，暂时忽略 case rabbit:https://bscscan.com/tx/0xaef1b0b3798e30debcae2f7f4fb4b21e1ac6038a4db197df56745ec18e48cf02
         select withdraw.block_number,
                withdraw.block_time as block_timestamp,
                withdraw.tx_hash    as transaction_hash,
                withdraw.log_index,
                withdraw.contract_address,
                withdraw.operator,
                tt.token_address    AS asset_address,
                CAST(tt.value AS BIGNUMERIC)            AS asset_amount,
                withdraw.pool_id
         from (select *
               from `footprint-etl.bsc_farming_pancakeswap.bsc_pancakeswap_farming_withdraw_all`
               where DATE (block_time) {match_date_filter}) withdraw
         left join `footprint-blockchain-etl.crypto_bsc.token_transfers` tt
                   on tt.transaction_hash = withdraw.tx_hash and
                      tt.to_address = withdraw.operator and
                      CAST(tt.value AS BIGNUMERIC) <> withdraw.token_amount_raw and
    DATE (tt.block_timestamp) {match_date_filter}

--     跟 deposit 一起拿到的奖励
UNION ALL
select supply.block_number,
       supply.block_time as block_timestamp,
       supply.tx_hash    as transaction_hash,
       supply.log_index,
       supply.contract_address,
       supply.operator,
       tt.token_address  as asset_address,
       CAST(tt.value AS BIGNUMERIC)          as asset_amount,
       supply.pool_id
from (select *
      from `footprint-etl.bsc_farming_pancakeswap.bsc_pancakeswap_farming_supply_all`
      where DATE (block_time) {match_date_filter}) supply
left join `footprint-blockchain-etl.crypto_bsc.token_transfers` tt
on tt.transaction_hash = supply.tx_hash and
    tt.to_address = supply.operator and
    CAST (tt.value AS BIGNUMERIC) <> supply.token_amount_raw and
    DATE (tt.block_timestamp) {match_date_filter}
    )
where asset_address is not null and Date (block_timestamp) {match_date_filter}
            