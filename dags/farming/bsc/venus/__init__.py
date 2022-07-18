from datetime import datetime, timedelta

from farming.bsc.venus.venus_farming_supply import VenusFarmingSupply
from farming.bsc.venus.venus_farming_withdraw import VenusFarmingWithdraw
from farming.bsc.venus.venus_farming_reward import VenusFarmingReward


VenusFarmingSupply = VenusFarmingSupply()
VenusFarmingWithdraw = VenusFarmingWithdraw()
VenusFarmingReward = VenusFarmingReward()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': VenusFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': VenusFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': VenusFarmingReward.run_daily_job},
    ]
