-- v1 需要从 withdraw event 取数
SELECT  'Sushiswap'                     AS project,
        '1'                             AS version,
        16                              AS protocol_id,
        'reward'                        AS type,
        reward.block_number,
        reward.block_timestamp,
        reward.transaction_hash,
        reward.log_index,
        reward.contract_address,
        reward.user                     AS operator,
        transfer.token_address          AS asset_address,
        CAST(transfer.value AS FLOAT64) AS asset_amount,
        lp.lpToken                      AS pool_id
FROM `footprint-etl.ethereum_sushi.MasterChef_event_Withdraw` reward
LEFT JOIN `footprint-etl.ethereum_sushi.pid_lp_token` lp
ON lp.version = '1' AND lp.pid = reward.pid
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  transfer.transaction_hash = reward.transaction_hash
  AND transfer.from_address = reward.contract_address
  AND transfer.to_address = reward.user
  AND transfer.token_address != lp.lpToken
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE transfer.value != '0' AND DATE(reward.block_timestamp) {match_date_filter}
UNION ALL
-- v2 有单独的 harvest event
SELECT  'Sushiswap'                     AS project,
        '2'                             AS version,
        16                              AS protocol_id,
        'reward'                        AS type,
        reward.block_number,
        reward.block_timestamp,
        reward.transaction_hash,
        reward.log_index,
        reward.contract_address,
        reward.user                     AS operator,
        transfer.token_address          AS asset_address,
        CAST(transfer.value AS FLOAT64) AS asset_amount,
        lp.lpToken                      AS pool_id
FROM `footprint-etl.ethereum_sushi.MasterChefV2_event_Harvest` reward
LEFT JOIN `footprint-etl.ethereum_sushi.pid_lp_token` lp
ON lp.version = '2' AND lp.pid = reward.pid
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  -- TODO https://etherscan.io/tx/0xd5ee553c185fcb451dd360b8f344e241d9172ab017d7d9f3cd48d5afcb2615fd
  transfer.transaction_hash = reward.transaction_hash
  AND transfer.to_address = reward.user
  AND transfer.token_address != lp.lpToken
  AND transfer.token_address != '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  AND DATE(transfer.block_timestamp) {match_date_filter}
)
WHERE transfer.value != '0' AND DATE(reward.block_timestamp) {match_date_filter}
