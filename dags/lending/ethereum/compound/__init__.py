from lending.ethereum.compound.compound_lending_borrow import CompoundLendingBorrow
from lending.ethereum.compound.compound_lending_repay import CompoundLendingRepay
from lending.ethereum.compound.compound_lending_supply import CompoundLendingSupply
from lending.ethereum.compound.compound_lending_withdraw import CompoundLendingWithdraw
from lending.ethereum.compound.compound_lending_liquidation import CompoundLendingLiquidation


CompoundLendingBorrow = CompoundLendingBorrow()
CompoundLendingRepay = CompoundLendingRepay()
CompoundLendingSupply = CompoundLendingSupply()
CompoundLendingWithdraw = CompoundLendingWithdraw()
CompoundLendingLiquidation = CompoundLendingLiquidation()


compound_business_list=[
    CompoundLendingBorrow,
    CompoundLendingRepay,
    CompoundLendingSupply,
    CompoundLendingWithdraw,
    CompoundLendingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': CompoundLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': CompoundLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': CompoundLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': CompoundLendingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': CompoundLendingLiquidation.run_daily_job},

    ]