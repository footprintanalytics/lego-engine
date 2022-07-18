from datetime import datetime, timedelta

from farming.ethereum.yearn.yearn_farming_withdraw import YearnFarmingWithdraw
from farming.ethereum.yearn.yearn_farming_supply import YearnFarmingSupply


YearnFarmingWithdraw = YearnFarmingWithdraw()
YearnFarmingSupply = YearnFarmingSupply()


def airflow_steps():
    return [
        {'type': 'Supply', 'func': YearnFarmingSupply.run_daily_job},
        {'type': 'Withdraw', 'func': YearnFarmingWithdraw.run_daily_job},
    ]
