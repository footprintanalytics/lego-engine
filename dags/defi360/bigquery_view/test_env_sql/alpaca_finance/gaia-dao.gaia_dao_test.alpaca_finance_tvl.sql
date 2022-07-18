select
a.day,
a.holder_address,
b.pool_id,
a.chain,
a.token_address,
sum(a.balance) as amount,
sum(a.balance * c.price) as tvl
from (
    select * from `gaia-dao.gaia_dao_test.alpaca_finance_borrow`
    union all
    select * from `footprint-test-341610.public_data.token_balances`
) a inner join `gaia-dao.gaia_dao.Alpaca_Finance_c21f58f3-adca-4f45-9027-bbc9111d9649_poolInfo_autoparse` b
on a.holder_address = b.deposit_contract_address_list[offset(0)]
left join `gaia-dao.gaia_dao.token_price` c
on lower(a.token_address) = lower(c.token_address) and a.chain = c.chain and date(a.day) = date(c.day)
group by 1,2,3,4,5