from lending.ethereum.raricapital.raricapital_lending_borrow import RaricapitalLendingBorrow
from lending.ethereum.raricapital.raricapital_lending_repay import RaricapitalLendingRepay
from lending.ethereum.raricapital.raricapital_lending_supply import RaricapitalLendingSupply
from lending.ethereum.raricapital.raricapital_lending_withdraw import RaricapitalLendingWithdraw
from lending.ethereum.raricapital.raricapital_lending_liquidation import RaricapitalLendingLiquidation


RaricapitalLendingBorrow = RaricapitalLendingBorrow()
RaricapitalLendingRepay = RaricapitalLendingRepay()
RaricapitalLendingSupply = RaricapitalLendingSupply()
RaricapitalLendingWithdraw = RaricapitalLendingWithdraw()
RaricapitalLendingLiquidation = RaricapitalLendingLiquidation()


raricapital_business_list=[
    RaricapitalLendingBorrow,
    RaricapitalLendingRepay,
    RaricapitalLendingSupply,
    RaricapitalLendingWithdraw,
    RaricapitalLendingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': RaricapitalLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': RaricapitalLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': RaricapitalLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': RaricapitalLendingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': RaricapitalLendingLiquidation.run_daily_job},

    ]