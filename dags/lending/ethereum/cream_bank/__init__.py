from lending.ethereum.cream_bank.cream_bank_lending_supply import CreambankLendingSupply
from lending.ethereum.cream_bank.cream_bank_lending_withdraw import CreambankLendingWithdraw


CreambankLendingSupply = CreambankLendingSupply()
CreambankLendingWithdraw = CreambankLendingWithdraw()

cream_bank_business_list=[
    CreambankLendingSupply,
    CreambankLendingWithdraw,
]

def airflow_steps():
	return [
		{'type': 'Supply', 'func': CreambankLendingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': CreambankLendingWithdraw.run_daily_job}
    ]