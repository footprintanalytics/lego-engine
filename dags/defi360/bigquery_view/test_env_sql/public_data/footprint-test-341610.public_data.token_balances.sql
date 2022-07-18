SELECT
    tbdd.chain,
    tbdd.holder_address,
    tbdd.token_address,
    tbdd.day,
    tbdd.balance / POW(10, IFNULL(t.decimals, 18)) AS balance
FROM (
    select chain,holder_address,token_address,day,balance from `footprint-test-341610.gaia_dao_test.token_balances_fortube_daily`
    union all
    select chain, holder_address,token_address,day,balance from `footprint-test-341610.gaia_dao_test.token_balance_alpaca_finance_daily`
) tbdd
LEFT JOIN `xed-project-237404.footprint_etl.erc20_all` t
ON tbdd.chain = t.chain AND lower(tbdd.token_address) = lower(t.contract_address)