with transactions as (
    select `hash`, from_address, to_address from `footprint-blockchain-etl.crypto_ethereum.transactions`
    where date(block_timestamp) >= '2021-01-28'
    and receipt_status = 1
)
SELECT
	'Alpha Finance' AS project,
    '2' AS version,
    61 AS protocol_id,
    'withdraw' AS type,
	block_number,
	block_timestamp,
	transaction_hash,
	log_index,
	redeem.redeemer as contract_address,
    transactions.from_address AS operator,
	pools.stake_token[offset(0)] AS asset_address,
	CAST(redeemAmount as BIGNUMERIC) AS asset_amount,
    redeem.redeemer as pool_id
FROM
	`footprint-etl.ethereum_cream_bank.CErc20Delegator_event_Redeem_alpha` as redeem
left join `xed-project-237404.footprint_etl.pool_infos` as pools on pools.pool_id = redeem.contract_address
left join transactions on transactions.`hash` = redeem.transaction_hash
WHERE
	Date(block_timestamp) {match_date_filter}