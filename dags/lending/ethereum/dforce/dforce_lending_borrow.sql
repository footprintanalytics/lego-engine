SELECT
	'dforce' as project,
	'1' as version,
	22 protocol_id,
	'lending' as type,
	b.block_number,
	b.block_timestamp as block_time,
	b.transaction_hash as tx_hash,
	b.log_index,
	contract_address,
	operator,
	if(contract_address='0x5acd75f21659a59ffab9aebaf350351a8bfaabc0','0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',token_address ) token_address,
	CAST(borrowAmount AS BIGNUMERIC) AS  token_amount_raw,
    concat(contract_address,'_22') as pool_id
FROM
	(select borrowAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,borrower as operator,'Main' pool_type from `footprint-etl.ethereum_dforce.lendingPool_event_Borrow`  where Date(block_timestamp) {match_date_filter}
-- 	union all
-- 	select borrowAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,borrower as operator,'Synth' pool_type from `footprint-etl.ethereum_dforce.iMSDPool_event_Borrow`  where Date(block_timestamp) {match_date_filter}
	)b left join
	(select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where Date(block_timestamp) {match_date_filter})t
	on t.transaction_hash=b.transaction_hash and t.value=b.borrowAmount and
	(
	(b.pool_type='Main' and t.from_address=b.contract_address)
	or (b.pool_type='Synth' and t.to_address=b.operator)
	)