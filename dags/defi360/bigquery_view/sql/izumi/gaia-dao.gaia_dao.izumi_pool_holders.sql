select * from `gaia-dao.gaia_dao.gaia__pool_holders` where pool_address in (
    select distinct deposit_contract_address from `gaia-dao.gaia_dao.izumi_36aa0168-59d2-4047-89bf-f7ff4075b22a_poolInfo_deposit_contract`
)