from lending.ethereum.cream.cream_lending_borrow import CreamLendingBorrow
from lending.ethereum.cream.cream_lending_repay import CreamLendingRepay
from lending.ethereum.cream.cream_lending_supply import CreamLendingSupply
from lending.ethereum.cream.cream_lending_withdraw import CreamLendingWithdraw
from lending.ethereum.cream.cream_lending_liquidation import CreamLendingLiquidation


CreamLendingBorrow = CreamLendingBorrow()
CreamLendingRepay = CreamLendingRepay()
CreamLendingSupply = CreamLendingSupply()
CreamLendingWithdraw = CreamLendingWithdraw()
CreamLendingLiquidation = CreamLendingLiquidation()


cream_business_list=[
    CreamLendingBorrow,
    CreamLendingRepay,
    CreamLendingSupply,
    CreamLendingWithdraw,
    CreamLendingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': CreamLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': CreamLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': CreamLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': CreamLendingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': CreamLendingLiquidation.run_daily_job},

    ]