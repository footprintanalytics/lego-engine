SELECT
	project,
	version,
	protocol_id,
	type,
	block_number,
	block_timestamp,
	transaction_hash,
	log_index,
	contract_address,
	borrower,
	asset_address,
	asset_amount
FROM
	view.liquidation
WHERE
	Date(block_timestamp) {match_date_filter}