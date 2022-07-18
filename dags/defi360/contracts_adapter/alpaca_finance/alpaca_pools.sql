select
chain,
pool_id,
deposit_contract_address_list[offset(0)] as deposit_address,
from `gaia-dao.gaia_dao.Alpaca_Finance_c21f58f3-adca-4f45-9027-bbc9111d9649_poolInfo_autoparse`