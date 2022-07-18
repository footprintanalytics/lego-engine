from lending.bsc.dforce.dforce_lending_borrow import DforceLendingBorrow
from lending.bsc.dforce.dforce_lending_repay import DforceLendingRepay
from lending.bsc.dforce.dforce_lending_supply import DforceLendingSupply
from lending.bsc.dforce.dforce_lending_withdraw import DforceLendingWithdraw


DforceLendingBorrow = DforceLendingBorrow()
DforceLendingRepay = DforceLendingRepay()
DforceLendingSupply = DforceLendingSupply()
DforceLendingWithdraw = DforceLendingWithdraw()


dforce_business_list=[
    DforceLendingBorrow,
    DforceLendingRepay,
    DforceLendingSupply,
    DforceLendingWithdraw,
]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': DforceLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': DforceLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': DforceLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': DforceLendingWithdraw.run_daily_job},

    ]