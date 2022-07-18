SELECT
	'abracadabra' as project,
	'1' as version,
	685 as  protocol_id,
	'borrow' as type,
	block_number,
	block_timestamp as block_time,
	transaction_hash as tx_hash,
	log_index,
	contract_address,
	borrower as operator,
	'0x130966628846bfd36ff31a822705796e8cb8c18d' as token_address,
	CAST(asset_amount AS BIGNUMERIC) AS token_amount_raw,
    pool_info.pool_id AS pool_id
FROM
	(
	    select
            block_number,
            block_timestamp,
            transaction_hash,
            log_index,
            contract_address,
            bor.to as borrower,
            bor.amount as asset_amount
	    from `footprint-etl.avalanche_abracadabra.CauldronV2Multichain_event_LogBorrow` as bor

    ) bor
LEFT JOIN `footprint-etl.footprint_pool_infos.pool_infos` pool_info ON pool_info.pool_id = bor.contract_address
WHERE
	Date(block_timestamp) {match_date_filter}