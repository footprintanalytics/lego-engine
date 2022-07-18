from datetime import datetime, timedelta

# from farming.ethereum.lido.lido_farming_reward import LidoFarmingReward
# from farming.ethereum.lido.lido_farming_withdraw import LidoFarmingWithdraw
from farming.ethereum.lido.lido_farming_supply import LidoFarmingSupply


# LidoFarmingReward = LidoFarmingReward()
# LidoFarmingWithdraw = LidoFarmingWithdraw()
LidoFarmingSupply = LidoFarmingSupply()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': LidoFarmingSupply.run_daily_job},
        # {'type': 'Withdraw', 'func': LidoFarmingWithdraw.run_daily_job},
        # {'type': 'Reward', 'func': LidoFarmingSupply.run_daily_job},
    ]
