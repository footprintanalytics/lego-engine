SELECT
	'Aave' AS project,
	'1' as version,
	791 as protocol_id,
	a.type,
	a.block_number,
	a.block_timestamp as block_time,
	a.transaction_hash as tx_hash,
	a.log_index,
	a.contract_address,
	a.borrower as operator,
	a.reserve as token_address,
	CAST(a.amount AS BIGNUMERIC) AS token_amount_raw,
	b.aToken as pool_id
FROM (
    SELECT 'lending' as type, block_number, block_timestamp, transaction_hash, log_index, contract_address, reserve, amount, user AS borrower
    FROM `footprint-etl.avalanche_aave.lendingPoolProxy_event_Borrow`

    UNION ALL

    SELECT 'flashloan' as type, block_number, block_timestamp, transaction_hash, log_index, contract_address, asset as reserve, amount, target AS borrower
    FROM `footprint-etl.avalanche_aave.lendingPoolProxy_event_FlashLoan`
) a
left join `footprint-etl.avalanche_aave.LendingPoolConfigurator_event_ReserveInitialized` b
on
    lower(a.reserve) = lower(b.asset)
WHERE
	Date(a.block_timestamp) {match_date_filter}