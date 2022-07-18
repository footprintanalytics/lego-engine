from farming.bsc.mdex.mdex_farming_supply import MdexFarmingSupply
from farming.bsc.mdex.mdex_farming_reward import MdexFarmingReward
from farming.bsc.mdex.mdex_farming_withdraw import MdexFarmingWithdraw


MdexFarmingSupply = MdexFarmingSupply()
MdexFarmingReward = MdexFarmingReward()
MdexFarmingWithdraw = MdexFarmingWithdraw()


def airflow_steps():
    return [
        {'type': 'Supply', 'func': MdexFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': MdexFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': MdexFarmingReward.run_daily_job},
    ]
