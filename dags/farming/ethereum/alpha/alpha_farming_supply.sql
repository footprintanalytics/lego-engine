with transactions as (
    select `hash`, from_address, to_address from `footprint-blockchain-etl.crypto_ethereum.transactions`
    where date(block_timestamp) >= '2021-01-28'
    and receipt_status = 1
)
SELECT
	'Alpha Finance' AS project,
    '2' AS version,
    61 AS protocol_id,
    'supply' AS type,
	block_number,
	block_timestamp,
	transaction_hash,
	log_index,
	mint.minter as contract_address,
    transactions.from_address AS operator,
	pools.stake_token[offset(0)] AS asset_address,
	CAST(mintAmount as BIGNUMERIC) AS asset_amount,
    mint.minter as pool_id
FROM
	`footprint-etl.ethereum_cream_bank.CErc20Delegator_event_Mint_alpha` as mint
left join `xed-project-237404.footprint_etl.pool_infos` as pools on pools.pool_id = mint.contract_address
left join transactions on transactions.`hash` = mint.transaction_hash
WHERE
	Date(block_timestamp) {match_date_filter}