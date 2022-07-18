select * from `gaia-data.gaia.pool_holders_daily` where pool_address in (
    select distinct deposit_contract_address_list[offset(0)] from `gaia-data.struct_data.izumi_pool_info`
)