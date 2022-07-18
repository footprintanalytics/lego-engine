from farming.ethereum.convex.convex_farming_supply import ConvexFarmingSupply
from farming.ethereum.convex.convex_farming_reward import ConvexFarmingReward
from farming.ethereum.convex.convex_farming_withdraw import ConvexFarmingWithdraw


ConvexFarmingSupply = ConvexFarmingSupply()
ConvexFarmingReward = ConvexFarmingReward()
ConvexFarmingWithdraw = ConvexFarmingWithdraw()


def airflow_steps():
    return [
        {'type': 'Supply', 'func': ConvexFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': ConvexFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': ConvexFarmingReward.run_daily_job},
    ]
