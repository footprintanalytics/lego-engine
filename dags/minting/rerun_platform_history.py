import datetime

import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm

from minting import all_project
from utils.date_util import DateUtil


def history_fixer(platform_class):
    assert datetime.datetime.now().hour not in range(10, 13) and datetime.datetime.now().weekday() != 4, '不允许在10-13点或者周五执行刷数据脚本, 要么就注释我!'
    pool = platform_class
    pool.delete_history_data(is_test=False)
    pool.parse_history_data()
    for dates in pd.date_range(start=pool.history_date, end=DateUtil.utc_24h_ago().strftime('%Y-%m-%d')).strftime(
            '%Y-%m-%d').to_list():
        pool.run_daily_job(date_str=dates)
    pool.create_all_data_view()


# merge_all_minting_view()


def history_lending_fixer(platform_class):
    pool = platform_class
    pool.run_daily_job()
    pool.parse_history_data()


def history_fixer_all(platform_class):
    try:
        history_fixer(platform_class)
    except Exception as e:
        print(platform_class.task_name)
        print(e)


if __name__ == '__main__':
    # Parallel(n_jobs=6)(delayed(history_fixer_all)(item) for item in tqdm(all_project))

    """执行demo"""
    from minting.ethereum.maker import (
        MakerMintingLiquidation
    )

    # history_fixer(AbracadabraMintingWithdraw)
    # history_fixer(AbracadabraMintingSupply)
    # history_fixer(AbracadabraMintingLiquidation)
    # history_fixer(AbracadabraMintingRepay)
    history_fixer(MakerMintingLiquidation)
