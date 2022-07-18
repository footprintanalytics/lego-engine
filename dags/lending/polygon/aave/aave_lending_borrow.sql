SELECT
	'Aave' AS project,
	'1' AS version,
	7 as protocol_id,
	type,
	block_number,
	block_timestamp as block_time,
	transaction_hash as tx_hash,
	log_index,
	contract_address,
	borrower as operator,
	reserve as token_address,
	CAST(amount AS BIGNUMERIC) AS token_amount_raw,
   '' as pool_id
FROM(
    SELECT 'lending' as type, block_number, block_timestamp, transaction_hash, log_index, contract_address, reserve, amount, user AS borrower
    FROM `footprint-etl.polygon_aave.AaveLendingPool_event_Borrow`

    UNION ALL

    SELECT 'flashloan' as type, block_number, block_timestamp, transaction_hash, log_index, contract_address, asset as reserve, amount, target AS borrower
    FROM `footprint-etl.polygon_aave.AaveLendingPool_event_FlashLoan`
)


WHERE
	Date(block_timestamp) {match_date_filter}