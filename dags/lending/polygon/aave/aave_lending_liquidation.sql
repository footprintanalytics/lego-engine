SELECT
	'Aave' AS project,
	'1' AS version,
	7 as protocol_id,
	'lending' as type,
	block_number,
	block_timestamp as block_time,
	transaction_hash as tx_hash,
	log_index,
	contract_address,
	borrower,
	collateralAsset as token_collateral_address,
	null as token_collateral_symbol,
	CAST(liquidatedCollateralAmount AS BIGNUMERIC) AS token_collateral_amount_raw,
	liquidator,
	debtAsset as repay_token_address,
	null as repay_token_symbol,
	CAST(debtToCover AS BIGNUMERIC) AS repay_token_amount_raw,
   '' as pool_id
FROM(
    SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, debtAsset, collateralAsset, liquidator , liquidatedCollateralAmount, debtToCover, user AS borrower
    FROM `footprint-etl.polygon_aave.AaveLendingPool_event_LiquidationCall`
)

WHERE
	Date(block_timestamp) {match_date_filter}
