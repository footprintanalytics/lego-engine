from lending.ethereum.bzx.bzx_lending_borrow import BzxLendingBorrow
from lending.ethereum.bzx.bzx_lending_repay import BzxLendingRepay
from lending.ethereum.bzx.bzx_lending_supply import BzxLendingSupply
from lending.ethereum.bzx.bzx_lending_withdraw import BzxLendingWithdraw


BzxLendingBorrow = BzxLendingBorrow()
BzxLendingRepay = BzxLendingRepay()
BzxLendingSupply = BzxLendingSupply()
BzxLendingWithdraw = BzxLendingWithdraw()


bzx_business_list=[
    BzxLendingBorrow,
    BzxLendingRepay,
    BzxLendingSupply,
    BzxLendingWithdraw,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': BzxLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': BzxLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': BzxLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': BzxLendingWithdraw.run_daily_job}
    ]