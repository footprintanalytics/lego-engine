with transactions as (
    select `hash`, from_address, to_address from `footprint-blockchain-etl.crypto_ethereum.transactions`
    where date(block_timestamp) >= '2021-04-28'
    and receipt_status = 1
)
SELECT
	'Alpha Finance' AS project,
    '2' AS version,
    61 AS protocol_id,
    'borrow' AS type,
	block_number,
	block_timestamp,
	transaction_hash,
	log_index,
	contract_address,
    CAST(borrow.positionId AS NUMERIC) AS positionId,
    transactions.from_address AS operator,
	token AS asset_address,
	CAST(amount as BIGNUMERIC) AS asset_amount,
    position_token.token_address as pool_id
FROM
	`footprint-etl.ethereum_alpha.HomoraBank_event_Borrow` as borrow
left join transactions on transactions.`hash` = borrow.transaction_hash
left join `footprint-etl.ethereum_farming_alpha.alpha_position_lp_token` as position_token on position_token.positionId  = CAST(borrow.positionId AS NUMERIC)
WHERE
	Date(block_timestamp) {match_date_filter}