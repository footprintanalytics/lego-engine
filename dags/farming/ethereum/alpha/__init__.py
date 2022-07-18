
from farming.ethereum.alpha.alpha_farming_supply import AlphaFarmingSupply
# from farming.ethereum.alpha.alpha_farming_reward import AlphaFarmingReward
from farming.ethereum.alpha.alpha_farming_withdraw import AlphaFarmingWithdraw
from farming.ethereum.alpha.alpha_farming_position_add import AlphaFarmingPositionAdd
from farming.ethereum.alpha.alpha_farming_position_remove import AlphaFarmingPositionRemove
from farming.ethereum.alpha.alpha_farming_collateral_add import AlphaFarmingCollateralAdd
from farming.ethereum.alpha.alpha_farming_collateral_remove import AlphaFarmingCollateralRemove
from farming.ethereum.alpha.alpha_farming_borrow import AlphaFarmingBorrow
from farming.ethereum.alpha.alpha_farming_repay import AlphaFarmingRepay
from farming.ethereum.alpha.alpha_farming_liquidation import AlphaFarmingLiquidation


AlphaFarmingSupply = AlphaFarmingSupply()
# AlphaFarmingReward = AlphaFarmingReward()
AlphaFarmingWithdraw = AlphaFarmingWithdraw()
AlphaFarmingPositionAdd = AlphaFarmingPositionAdd()
AlphaFarmingPositionRemove = AlphaFarmingPositionRemove()
AlphaFarmingCollateralAdd = AlphaFarmingCollateralAdd()
AlphaFarmingCollateralRemove = AlphaFarmingCollateralRemove()
AlphaFarmingBorrow = AlphaFarmingBorrow()
AlphaFarmingRepay = AlphaFarmingRepay()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': AlphaFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': AlphaFarmingWithdraw.run_daily_job},
        # {'type': 'Reward', 'func': AlphaFarmingWithdraw.run_daily_job},
        {'type': 'PositionAdd', 'func': AlphaFarmingPositionAdd.run_daily_job},
        {'type': 'PositionRemove', 'func': AlphaFarmingPositionRemove.run_daily_job},
        {'type': 'CollateralAdd', 'func': AlphaFarmingCollateralAdd.run_daily_job},
        {'type': 'CollateralRemove', 'func': AlphaFarmingCollateralRemove.run_daily_job},
        {'type': 'Borrow', 'func': AlphaFarmingBorrow.run_daily_job},
        {'type': 'Repay', 'func': AlphaFarmingRepay.run_daily_job},
    ]
