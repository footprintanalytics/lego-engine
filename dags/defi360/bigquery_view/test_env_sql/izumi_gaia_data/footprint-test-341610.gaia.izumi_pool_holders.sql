select * from `footprint-test-341610.gaia.pool_holders_daily` where pool_address in (
    select distinct deposit_contract_address_list[offset(0)] from `footprint-test-341610.struct_data.izumi_pool_info`
)