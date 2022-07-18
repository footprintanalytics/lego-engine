SELECT
    tbdd.chain,
    lower(tbdd.holder_address) as holder_address,
    lower(tbdd.token_address) as token_address,
    tbdd.day,
    tbdd.balance / POW(10, IFNULL(t.decimals, 18)) AS balance
FROM (
    select chain,holder_address,token_address,day,balance from `gaia-dao.public_data.token_balances_fortube_daily`
    union all
    select chain, holder_address,token_address,day,balance from `gaia-dao.public_data.token_balances_alpaca_finance_daily`
    union all
    select chain, holder_address,token_address,day,balance from `gaia-dao.public_data.token_balances_dodo_daily`
) tbdd
LEFT JOIN `xed-project-237404.footprint_etl.erc20_all` t
ON tbdd.chain = t.chain AND lower(tbdd.token_address) = lower(t.contract_address)