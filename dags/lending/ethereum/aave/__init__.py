from lending.ethereum.aave.aave_lending_borrow import AaveLendingBorrow
from lending.ethereum.aave.aave_lending_repay import AaveLendingRepay
from lending.ethereum.aave.aave_lending_supply import AaveLendingSupply
from lending.ethereum.aave.aave_lending_withdraw import AaveLendingWithdraw
from lending.ethereum.aave.aave_lending_liquidation import AaveLendingLiquidation


AaveLendingBorrow = AaveLendingBorrow()
AaveLendingRepay = AaveLendingRepay()
AaveLendingSupply = AaveLendingSupply()
AaveLendingWithdraw = AaveLendingWithdraw()
AaveLendingLiquidation = AaveLendingLiquidation()


aave_business_list=[
    AaveLendingBorrow,
    AaveLendingRepay,
    AaveLendingSupply,
    AaveLendingWithdraw,
    AaveLendingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': AaveLendingBorrow.run_daily_job},
		{'type': 'Repay', 'func': AaveLendingRepay.run_daily_job},
		{'type': 'Supply', 'func': AaveLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': AaveLendingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': AaveLendingLiquidation.run_daily_job},

    ]