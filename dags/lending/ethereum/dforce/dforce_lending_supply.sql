SELECT
	'dforce' as project,
	'1' as version,
	22 protocol_id,
	'lending' as type,
	m.block_number,
	m.block_timestamp as block_time,
	m.transaction_hash as tx_hash,
	m.log_index,
	contract_address,
	operator,
	if(contract_address='0x5acd75f21659a59ffab9aebaf350351a8bfaabc0','0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',token_address )token_address,
	CAST(mintAmount AS BIGNUMERIC) AS token_amount_raw,
    concat(contract_address,'_22') as pool_id
FROM
	(select mintAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,sender as operator,'Main' pool_type  from `footprint-etl.ethereum_dforce.lendingPool_event_Mint` where Date(block_timestamp) {match_date_filter}
-- 	union all
-- 	select  mintAmount,transaction_hash,contract_address,log_index,block_number,block_timestamp,sender as operator,'Synth' pool_type  from `footprint-etl.ethereum_dforce.iMSDPool_event_Mint` where Date(block_timestamp) {match_date_filter}
	)m left join
	(select * from `footprint-blockchain-etl.crypto_ethereum.token_transfers` where Date(block_timestamp) {match_date_filter})t
	on t.transaction_hash=m.transaction_hash and t.value=m.mintAmount and
	((m.pool_type='Main'  and t.to_address=m.contract_address) or (m.pool_type='Synth' and t.from_address= m.operator))
