from minting.ethereum.frax.frax_minting_borrow import FraxMintingBorrow
from minting.ethereum.frax.frax_minting_repay import FraxMintingRepay
from minting.ethereum.frax.frax_minting_supply import FraxMintingSupply
from minting.ethereum.frax.frax_minting_withdraw import FraxMintingWithdraw


FraxMintingBorrow = FraxMintingBorrow()
FraxMintingRepay = FraxMintingRepay()
FraxMintingSupply = FraxMintingSupply()
FraxMintingWithdraw = FraxMintingWithdraw()


frax_business_list=[
    FraxMintingBorrow,
    FraxMintingRepay,
    FraxMintingSupply,
    FraxMintingWithdraw,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': FraxMintingBorrow.run_daily_job},
		{'type': 'Repay', 'func': FraxMintingRepay.run_daily_job},
		{'type': 'Supply', 'func': FraxMintingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': FraxMintingWithdraw.run_daily_job},

    ]