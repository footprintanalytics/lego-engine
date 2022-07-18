with transactions as (
    select `hash`, from_address, to_address from `footprint-blockchain-etl.crypto_ethereum.transactions`
    where date(block_timestamp) >= '2021-05-10'
    and to_address = '0xba5ebaf3fc1fcca67147050bf80462393814e54b'
    and receipt_status = 1
),
token_transfer as (
    select transaction_hash, token_address, value, from_address, to_address
    from `footprint-blockchain-etl.crypto_ethereum.token_transfers`
    where date(block_timestamp) >= '2021-05-10'
    and from_address in (
        '0xdc9c7a2bae15dd89271ae5701a6f4db147baa44c',
        '0xdff0d5f17ae42f9c6f1dfc1d94568f650d36c6e8',
        '0x6b8079bf80e07fb103a55e49ee226f729e1e38d5',
        '0x8b947d8448cffb89ef07a6922b74fbabac219795',
        '0x7b1f4cdd4f599774feae6516460bccd97fc2100e',
        '0xc4a59cfed3fe06bdb5c21de75a70b20db280d8fe',
        '0x00b1a4e7f217380a7c9e6c12f327ac4a1d9b6a14')
),
add_flow as (
    select
    block_number,
	block_timestamp,
	transaction_hash,
	log_index,
	contract_address,
    positionId,
    transactions.from_address AS operator,
    caller,
    from `footprint-etl.ethereum_alpha.HomoraBank_event_TakeCollateral` as collateral
    left join transactions on transactions.`hash` = collateral.transaction_hash
)

select * from (
    select
        'Alpha Finance' AS project,
        '2' AS version,
        61 AS protocol_id,
        add_flow.block_number,
        add_flow.block_timestamp,
        add_flow.transaction_hash,
        add_flow.log_index,
        add_flow.contract_address,
        CAST(add_flow.positionId AS NUMERIC) AS positionId,
        add_flow.operator,
        token_transfer.token_address AS asset_address,
        CAST(value as BIGNUMERIC) AS asset_amount,
        position_token.token_address as pool_id
    from add_flow
    left join token_transfer
    on (add_flow.transaction_hash = token_transfer.transaction_hash
    and add_flow.operator = token_transfer.to_address
    and add_flow.caller = token_transfer.from_address)
    left join `footprint-etl.ethereum_farming_alpha.alpha_position_lp_token` as position_token on position_token.positionId  = CAST(add_flow.positionId AS NUMERIC)
) as res
where res.asset_address is not null

AND
	Date(block_timestamp) {match_date_filter}