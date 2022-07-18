from lending.bsc.venus.venus_lending_borrow import VenusLendingBorrow
from lending.bsc.venus.venus_lending_repay import VenusLendingRepay
from lending.bsc.venus.venus_lending_supply import VenusLendingSupply
from lending.bsc.venus.venus_lending_withdraw import VenusLendingWithdraw
from lending.bsc.venus.venus_lending_liquidation import VenusLendingLiquidation


VenusLendingBorrow = VenusLendingBorrow()
VenusLendingRepay = VenusLendingRepay()
VenusLendingSupply = VenusLendingSupply()
VenusLendingWithdraw = VenusLendingWithdraw()
VenusLendingLiquidation = VenusLendingLiquidation()


venus_business_list=[
    VenusLendingBorrow,
    VenusLendingRepay,
    VenusLendingSupply,
    VenusLendingWithdraw,
    VenusLendingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': VenusLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': VenusLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': VenusLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': VenusLendingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': VenusLendingLiquidation.run_daily_job},

    ]