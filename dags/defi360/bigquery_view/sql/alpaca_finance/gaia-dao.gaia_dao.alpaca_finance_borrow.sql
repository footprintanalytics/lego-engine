SELECT
    tbdd.chain,
    tbdd.holder_address,
    tbdd.token_address,
    tbdd.day,
    tbdd.balance / POW(10, IFNULL(t.decimals, 18)) AS balance
FROM (
    select * from `footprint-test-341610.gaia_dao_test.borrow_token_balance_alpaca_finance_daily`
) tbdd
LEFT JOIN
    `xed-project-237404.footprint_etl.erc20_all` t
ON
    tbdd.chain = t.chain AND tbdd.token_address = t.contract_address