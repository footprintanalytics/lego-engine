from farming.ethereum.sushi.sushi_farming_supply import SushiFarmingSupply
from farming.ethereum.sushi.sushi_farming_reward import SushiFarmingReward
from farming.ethereum.sushi.sushi_farming_withdraw import SushiFarmingWithdraw


SushiFarmingSupply = SushiFarmingSupply()
SushiFarmingReward = SushiFarmingReward()
SushiFarmingWithdraw = SushiFarmingWithdraw()


def airflow_steps():
    return [
        {'type': 'Supply', 'func': SushiFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': SushiFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': SushiFarmingReward.run_daily_job},
    ]
