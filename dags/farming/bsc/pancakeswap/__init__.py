from datetime import datetime, timedelta

from farming.bsc.pancakeswap.pancakeswap_farming_supply import PancakeswapFarmingSupply
from farming.bsc.pancakeswap.pancakeswap_farming_reward import PancakeswapFarmingReward
from farming.bsc.pancakeswap.pancakeswap_farming_withdraw import PancakeswapFarmingWithdraw


PancakeswapFarmingSupply = PancakeswapFarmingSupply()
PancakeswapFarmingReward = PancakeswapFarmingReward()
PancakeswapFarmingWithdraw = PancakeswapFarmingWithdraw()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': PancakeswapFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': PancakeswapFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': PancakeswapFarmingReward.run_daily_job},
    ]
