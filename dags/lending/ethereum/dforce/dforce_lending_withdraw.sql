SELECT
	'dforce' as project,
	'1' as version,
	22 protocol_id,
	'lending' as type,
	r.block_number,
	r.block_timestamp as block_time,
	r.transaction_hash as tx_hash,
	r.log_index,
	contract_address,
	operator,
	if(contract_address='0x5acd75f21659a59ffab9aebaf350351a8bfaabc0','0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',token_address ) token_address,
	CAST(redeemUnderlyingAmount AS BIGNUMERIC) AS token_amount_raw,
    concat(contract_address,'_22') as pool_id
FROM
	(select redeemUnderlyingAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,recipient as operator,'Main' pool_type from `footprint-etl.ethereum_dforce.lendingPool_event_Redeem`  where Date(block_timestamp) {match_date_filter}
-- 	union all
-- 	select redeemUnderlyingAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,recipient as operator ,'Synth' pool_type from `footprint-etl.ethereum_dforce.iMSDPool_event_Redeem`  where Date(block_timestamp) {match_date_filter}
	)r left join
	(select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where Date(block_timestamp) {match_date_filter})t
	on t.transaction_hash=r.transaction_hash and t.value=r.redeemUnderlyingAmount and
	((r.pool_type='Main' and t.from_address=r.contract_address ) or (r.pool_type='Synth' and t.to_address=r.operator))