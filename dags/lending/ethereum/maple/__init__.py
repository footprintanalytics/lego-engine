from lending.ethereum.maple.maple_lending_supply import MapleLendingSupply
from lending.ethereum.maple.maple_lending_withdraw import MapleLendingWithdraw



MapleLendingSupply = MapleLendingSupply()
MapleLendingWithdraw = MapleLendingWithdraw()


maple_business_list=[
    MapleLendingSupply,
    MapleLendingWithdraw,

]

def airflow_steps():
	return [
		{'type': 'Supply', 'func': MapleLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': MapleLendingWithdraw.run_daily_job},
    ]