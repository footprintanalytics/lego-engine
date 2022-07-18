SELECT  'Compound'                                                                  AS project,
        '2'                                                                         AS version,
        10                                                                          AS protocol_id,
        'lending'                                                               AS type,
        liquidate.block_number,
        liquidate.block_timestamp as block_time,
        liquidate.transaction_hash as tx_hash,
        liquidate.log_index,
        liquidate.contract_address,
        liquidate.borrower,
        liquidate.cTokenCollateral                                                  AS token_collateral_address,
        CAST(liquidate.seizeTokens AS numeric)                                      AS token_collateral_amount_raw,
        liquidate.liquidator,
        IFNULL(transfer.token_address,'0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS repay_token_address,
        CAST(liquidate.repayAmount AS numeric)                                      AS repay_token_amount_raw,
       liquidate.contract_address as pool_id
FROM `footprint-etl.ethereum_compound.cToken_event_LiquidateBorrow` liquidate
LEFT JOIN `footprint-blockchain-etl.crypto_ethereum.token_transfers` transfer
ON (
  transfer.transaction_hash = liquidate.transaction_hash
  AND transfer.to_address = liquidate.contract_address
  AND transfer.value = liquidate.repayAmount
  AND DATE (transfer.block_timestamp) {match_date_filter}
)
WHERE DATE (liquidate.block_timestamp) {match_date_filter}
