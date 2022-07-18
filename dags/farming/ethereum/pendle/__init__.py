from datetime import datetime, timedelta

from farming.ethereum.pendle.pendle_farming_supply import PendleFarmingSupply
from farming.ethereum.pendle.pendle_farming_reward import PendleFarmingReward
from farming.ethereum.pendle.pendle_farming_withdraw import PendleFarmingWithdraw


PendleFarmingSupply = PendleFarmingSupply()
PendleFarmingReward = PendleFarmingReward()
PendleFarmingWithdraw = PendleFarmingWithdraw()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': PendleFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': PendleFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': PendleFarmingReward.run_daily_job}
    ]
