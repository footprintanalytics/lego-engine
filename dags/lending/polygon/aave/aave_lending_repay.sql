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
	CAST(asset_amount AS BIGNUMERIC) as token_amount_raw,
   '' as pool_id
FROM(
    SELECT 'lending' as type, block_number, block_timestamp, transaction_hash, log_index, contract_address, reserve, amount AS asset_amount, user AS borrower
    FROM `footprint-etl.polygon_aave.AaveLendingPool_event_Repay`

    union all

    SELECT 'flashloan' as type, block_number, block_timestamp, transaction_hash, log_index, contract_address, asset as reserve, amount AS asset_amount, target AS borrower
    FROM `footprint-etl.polygon_aave.AaveLendingPool_event_FlashLoan`
)
WHERE
	Date(block_timestamp) {match_date_filter}