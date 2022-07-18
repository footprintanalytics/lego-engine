with transactions as (
    select `hash`, from_address, to_address from `footprint-blockchain-etl.crypto_ethereum.transactions`
    where date(block_timestamp) >= '2021-01-28'
    and receipt_status = 1
)
SELECT
	'Cream' AS project,
    'iron_bank' AS version,
    18 AS protocol_id,
    'lending' AS type,
	block_number,
	block_timestamp as block_time,
	transaction_hash as tx_hash,
	log_index,
	contract_address,
    transactions.from_address AS operator,
	pools.stake_token AS token_address,
	CAST(mintAmount as BIGNUMERIC) AS token_amount_raw,
    mint.contract_address AS pool_id
FROM
	`footprint-etl.ethereum_cream_bank.CErc20Delegator_event_Mint_alpha` as mint
left join `xed-project-237404.footprint_etl.pool_infos` as pools on pools.pool_id = mint.contract_address
left join transactions on transactions.`hash` = mint.transaction_hash
WHERE
	Date(block_timestamp) {match_date_filter}