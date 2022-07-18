select
chain,
pool_id,
deposit_contract_address_list as deposit_address,
from `gaia-data.struct_data.alpaca_finance_pool_info`
where defi_category = 'Lending'