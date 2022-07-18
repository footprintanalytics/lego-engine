SELECT
	'dforce' as project,
	'1' as version,
	22 protocol_id,
	'lending' as type,
	l.block_number,
	l.block_timestamp as block_time,
	l.transaction_hash as tx_hash,
	l.log_index,
	contract_address,
	borrower,
	iTokenCollateral as token_collateral_address,
	CAST(seizeTokens AS BIGNUMERIC) AS  token_collateral_amount_raw,
	liquidator,
	if(contract_address='0x5acd75f21659a59ffab9aebaf350351a8bfaabc0','0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',token_address ) repay_token_address,
	CAST(repayAmount AS BIGNUMERIC) AS  repay_token_amount_raw,
    concat(contract_address,'_22') as pool_id
FROM
	(select repayAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,liquidator,borrower,iTokenCollateral,seizeTokens,'Main' pool_type from `footprint-etl.ethereum_dforce.lendingPool_event_LiquidateBorrow`  where Date(block_timestamp) {match_date_filter}
-- 	union all
-- 	select repayAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,liquidator,borrower,iTokenCollateral,seizeTokens,'Synth' pool_type from `footprint-etl.ethereum_dforce.iMSDPool_event_LiquidateBorrow`  where Date(block_timestamp) {match_date_filter}
	)l left join
	(select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where Date(block_timestamp) {match_date_filter})t
	on t.transaction_hash=l.transaction_hash and t.value=l.repayAmount and
	(
	( l.pool_type='Main' and t.to_address=l.contract_address)or
	(l.pool_type='Synth' and t.from_address=l.liquidator)
	)
