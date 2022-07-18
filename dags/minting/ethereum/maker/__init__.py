from minting.ethereum.maker.maker_minting_borrow import MakerMintingBorrow
from minting.ethereum.maker.maker_minting_repay import MakerMintingRepay
from minting.ethereum.maker.maker_minting_supply import MakerMintingSupply
from minting.ethereum.maker.maker_minting_withdraw import MakerMintingWithdraw
from minting.ethereum.maker.maker_minting_liquidation import MakerMintingLiquidation


MakerMintingBorrow = MakerMintingBorrow()
MakerMintingRepay = MakerMintingRepay()
MakerMintingSupply = MakerMintingSupply()
MakerMintingWithdraw = MakerMintingWithdraw()
MakerMintingLiquidation = MakerMintingLiquidation()


maker_business_list=[
    MakerMintingBorrow,
    MakerMintingRepay,
    MakerMintingSupply,
    MakerMintingWithdraw,
    MakerMintingLiquidation,

]

def airflow_steps():
	return [
		{'type': 'Borrow', 'func': MakerMintingBorrow.run_daily_job},
		{'type': 'Repay', 'func': MakerMintingRepay.run_daily_job},
		{'type': 'Supply', 'func': MakerMintingSupply.run_daily_job},
		{'type': 'Withdraw', 'func': MakerMintingWithdraw.run_daily_job},
		{'type': 'Liquidation', 'func': MakerMintingLiquidation.run_daily_job},

    ]