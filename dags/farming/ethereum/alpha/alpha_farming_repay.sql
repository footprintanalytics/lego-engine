with transactions as (
    select `hash`, from_address, to_address from `footprint-blockchain-etl.crypto_ethereum.transactions`
    where date(block_timestamp) >= '2021-05-10'
    and receipt_status = 1
)
SELECT
	'Alpha Finance' AS project,
    '2' AS version,
    61 AS protocol_id,
    'repay' AS type,
	block_number,
	block_timestamp,
	transaction_hash,
	log_index,
	contract_address,
    CAST(repay.positionId AS NUMERIC) AS positionId,
    transactions.from_address AS operator,
	token AS asset_address,
	CAST(amount as BIGNUMERIC) AS asset_amount,
    position_token.token_address as pool_id
FROM
	`footprint-etl.ethereum_alpha.HomoraBank_event_Repay` as repay
left join transactions on transactions.`hash` = repay.transaction_hash
left join `footprint-etl.ethereum_farming_alpha.alpha_position_lp_token` as position_token on position_token.positionId  = CAST(repay.positionId AS NUMERIC)
WHERE
	Date(block_timestamp) {match_date_filter}