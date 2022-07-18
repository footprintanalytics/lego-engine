select * from `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a`
union all
select
    project,
    chain,
    protocol_id,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    contract_address,
    operator,
    token_symbol,
    token_address,
    token_amount,
    safe_cast(token_amount_raw as float64) token_amount_raw,
    pool_id,
    name,
    version,
    business_type,
    usd_value
    from `gaia-dao.gaia_dao.izumi_one_side`