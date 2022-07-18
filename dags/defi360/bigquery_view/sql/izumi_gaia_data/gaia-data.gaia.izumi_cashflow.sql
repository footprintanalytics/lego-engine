select
    protocol_name,
    chain,
    protocol_slug,
    business_type,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    contract_address,
    operator,
    token_symbol,
    token_address,
    token_amount,
    token_amount_raw,
    pool_id,
    pool_name,
    pool_version,
    usd_value,

    protocol_name as project,
    protocol_slug as protocol_id,
    pool_name as name,
    pool_version as version
from `gaia-data.struct_data.izumi_cashflow`
union all
select
    project as protocol_name,
    chain,
    project as protocol_slug,
    business_type,
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
    name as pool_name,
    version as pool_version,
    usd_value,

    project,
    protocol_id,
    name,
    version
    from `gaia-data.gaia.izumi_one_side`