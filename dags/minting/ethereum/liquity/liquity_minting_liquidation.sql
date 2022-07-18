SELECT
	'Liquity' as project,
	'1' as version,
	183 as protocol_id,
	'minting' as type,
	block_number,
	block_timestamp as block_time,
	transaction_hash as tx_hash,
	log_index,
	contract_address as contract_address,
	_borrower as borrower,
	'0x5f98805a4e8be255a32880fdec7f6728c6568ba0' as token_collateral_address,
	cast(_debt as BIGNUMERIC) as token_collateral_amount_raw,
	contract_address as liquidator,
	'eth' as repay_token_address,
	cast(_coll as BIGNUMERIC) as repay_token_amount_raw,
     '' as pool_id
FROM
	`footprint-etl-internal.ethereum_liquity.TroveManager_event_TroveLiquidated`
WHERE
	Date(block_timestamp) {match_date_filter}
