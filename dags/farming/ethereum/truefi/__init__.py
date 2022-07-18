from datetime import datetime, timedelta

from farming.ethereum.truefi.truefi_farming_supply import TruefiFarmingSupply
from farming.ethereum.truefi.truefi_farming_reward import TruefiFarmingReward
from farming.ethereum.truefi.truefi_farming_withdraw import TruefiFarmingWithdraw


TruefiFarmingSupply = TruefiFarmingSupply()
TruefiFarmingReward = TruefiFarmingReward()
TruefiFarmingWithdraw = TruefiFarmingWithdraw()



def airflow_steps():
    return [
        {'type': 'Supply', 'func': TruefiFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': TruefiFarmingWithdraw.run_daily_job},
        {'type': 'Reward', 'func': TruefiFarmingReward.run_daily_job},
    ]
