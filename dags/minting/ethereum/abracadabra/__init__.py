from minting.ethereum.abracadabra.abracadabra_minting_borrow import AbracadabraMintingBorrow
from minting.ethereum.abracadabra.abracadabra_minting_repay import AbracadabraMintingRepay
from minting.ethereum.abracadabra.abracadabra_minting_supply import AbracadabraMintingSupply
from minting.ethereum.abracadabra.abracadabra_minting_withdraw import AbracadabraMintingWithdraw
from minting.ethereum.abracadabra.abracadabra_minting_liquidation import AbracadabraMintingLiquidation


AbracadabraMintingBorrow = AbracadabraMintingBorrow()
AbracadabraMintingRepay = AbracadabraMintingRepay()
AbracadabraMintingSupply = AbracadabraMintingSupply()
AbracadabraMintingWithdraw = AbracadabraMintingWithdraw()
AbracadabraMintingLiquidation = AbracadabraMintingLiquidation()


abracadabra_business_list=[
    AbracadabraMintingBorrow,
    AbracadabraMintingRepay,
    AbracadabraMintingSupply,
    AbracadabraMintingWithdraw,
    AbracadabraMintingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': AbracadabraMintingBorrow.run_daily_job},
		{'type': 'Repay', 'func': AbracadabraMintingRepay.run_daily_job},
		{'type': 'Supply', 'func': AbracadabraMintingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': AbracadabraMintingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': AbracadabraMintingLiquidation.run_daily_job},

    ]