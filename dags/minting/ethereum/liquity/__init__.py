from minting.ethereum.liquity.liquity_minting_borrow import LiquityMintingBorrow
from minting.ethereum.liquity.liquity_minting_repay import LiquityMintingRepay
from minting.ethereum.liquity.liquity_minting_supply import LiquityMintingSupply
from minting.ethereum.liquity.liquity_minting_withdraw import LiquityMintingWithdraw
from minting.ethereum.liquity.liquity_minting_liquidation import LiquityMintingLiquidation


LiquityMintingBorrow = LiquityMintingBorrow()
LiquityMintingRepay = LiquityMintingRepay()
LiquityMintingSupply = LiquityMintingSupply()
LiquityMintingWithdraw = LiquityMintingWithdraw()
LiquityMintingLiquidation = LiquityMintingLiquidation()


liquity_business_list=[
    LiquityMintingBorrow,
    LiquityMintingRepay,
    LiquityMintingSupply,
    LiquityMintingWithdraw,
    LiquityMintingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': LiquityMintingBorrow.run_daily_job},
		{'type': 'Repay', 'func': LiquityMintingRepay.run_daily_job},
		{'type': 'Supply', 'func': LiquityMintingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': LiquityMintingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': LiquityMintingLiquidation.run_daily_job},

    ]