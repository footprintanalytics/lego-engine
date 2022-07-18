from lending.ethereum.truefi.truefi_lending_borrow import TruefiLendingBorrow
from lending.ethereum.truefi.truefi_lending_repay import TruefiLendingRepay
from lending.ethereum.truefi.truefi_lending_supply import TruefiLendingSupply
from lending.ethereum.truefi.truefi_lending_withdraw import TruefiLendingWithdraw


TruefiLendingBorrow = TruefiLendingBorrow()
TruefiLendingRepay = TruefiLendingRepay()
TruefiLendingSupply = TruefiLendingSupply()
TruefiLendingWithdraw = TruefiLendingWithdraw()


truefi_business_list=[
    TruefiLendingBorrow,
    TruefiLendingRepay,
    TruefiLendingSupply,
    TruefiLendingWithdraw,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': TruefiLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': TruefiLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': TruefiLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': TruefiLendingWithdraw.run_daily_job}
    ]