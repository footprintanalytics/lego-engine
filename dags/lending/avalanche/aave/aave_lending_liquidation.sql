SELECT
	'Aave' AS project,
	'1' as version,
	791 as protocol_id,
	'lending' as type,
	a.block_number,
	a.block_timestamp as block_time,
	a.transaction_hash as tx_hash,
	a.log_index,
	a.contract_address,
	a.borrower,
	a.collateralAsset as token_collateral_address,
	null as token_collateral_symbol,
	CAST(a.liquidatedCollateralAmount AS BIGNUMERIC) AS token_collateral_amount_raw,
	a.liquidator,
	a.debtAsset as repay_token_address,
	null as repay_token_symbol,
	CAST(a.debtToCover AS BIGNUMERIC) AS repay_token_amount_raw,
   b.aToken as pool_id
FROM(
    SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, debtAsset, collateralAsset, liquidator , liquidatedCollateralAmount, debtToCover, user AS borrower
    FROM `footprint-etl.avalanche_aave.lendingPoolProxy_event_LiquidationCall`
) a
LEFT JOIN `footprint-etl.avalanche_aave.LendingPoolConfigurator_event_ReserveInitialized` b
ON
    lower(a.debtAsset) = lower(b.asset)
WHERE
	Date(a.block_timestamp) {match_date_filter}
