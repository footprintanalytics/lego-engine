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
	a.borrower as operator,
	a.reserve as token_address,
	CAST(a.amount AS float64 ) AS token_amount_raw,
   b.aToken as pool_id
FROM
	(
	    SELECT block_number, block_timestamp, transaction_hash, log_index, contract_address, reserve, amount, user AS borrower
        FROM `footprint-etl.avalanche_aave.lendingPoolProxy_event_Deposit`
	) a
LEFT JOIN `footprint-etl.avalanche_aave.LendingPoolConfigurator_event_ReserveInitialized` b
ON
    lower(a.reserve) = lower(b.asset)
WHERE
	Date(a.block_timestamp) {match_date_filter}