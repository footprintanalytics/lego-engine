from datetime import datetime, timedelta

from farming.ethereum.harvest.harvest_farming_supply import HarvestFarmingSupply
from farming.ethereum.harvest.harvest_farming_reward import HarvestFarmingReward
from farming.ethereum.harvest.harvest_farming_withdraw import HarvestFarmingWithdraw


HarvestFarmingSupply = HarvestFarmingSupply()
HarvestFarmingReward = HarvestFarmingReward()
HarvestFarmingWithdraw = HarvestFarmingWithdraw()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': HarvestFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': HarvestFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': HarvestFarmingReward.run_daily_job}
    ]
