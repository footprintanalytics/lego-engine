import os

import pandas as pd
import pydash

from config import project_config
from defi360.common.contract_adapter import ContractAdapter
from defi360.utils.file_cash import FileCache
from defi360.utils.multicall import load_abi, multi_call
from utils.common import read_file
from utils.query_bigquery import query_bigquery


class TokenBalancesDODO(ContractAdapter):
    # 自己确定数据集
    data_set = "public_data"
    task_name = "token_balances_dodo"
    # time_partitioning_field = "day"
    time_partitioning_field = None
    execution_time = "35 2,6 * * *"
    history_date = '2022-02-21'
    schema_name = "defi360/schema/token_balances.json"
    depend_sql_path = 'defi360/contracts_adapter/token_balances/depend_data.sql'
    history_day = 10
    abi = 'defi360/contracts_adapter/abi/erc20.json'

    def get_depend_data(self, run_date: str):
        dags_folder = project_config.dags_folder
        sql_path = os.path.join(dags_folder, self.depend_sql_path)
        query_string = read_file(sql_path)
        try:
            dep_data = query_bigquery(query_string)
            return dep_data
        except Exception as e:
            print(e)

        return []

    def get_depend_data_with_cache(self, run_date: str):
        # dep_data_file_name = 'defi360/contracts_adapter/cache/{}_{}_dep_cache.csv'.format(self.task_name, run_date)
        dep_data_file_name = 'defi360/contracts_adapter/cache/{}_dep_cache.csv'.format(self.task_name)
        file_cache = FileCache()
        if file_cache.exist_cash_name(dep_data_file_name):
            return file_cache.get_csv_data(dep_data_file_name)

        dep_data = self.get_depend_data(run_date)
        file_cache.cash_csv_data(dep_data, dep_data_file_name)
        return file_cache.get_csv_data(dep_data_file_name)

    # 各自实现合约取数逻辑
    def get_daily_data(self, run_date: str, block_numbers: list):
        pools_data = self.get_depend_data_with_cache(run_date)

        # print(pools_data)
        token_balances = []
        for chain, pools in pools_data.groupby('chain'):
            tbs = []
            try:
                block_number = pydash.get(pydash.find(block_numbers, {"chain": chain.lower(), "date": run_date}), "block")
                tbs = self.get_token_balances(pools, chain, run_date, block_number)
            except Exception as e:
                print('get_token_balances error: ', e)
            token_balances += tbs
        df = pd.DataFrame(token_balances)
        return df

    # return pydash.flatten(pydash.map_(res, 'output'))
    # 获取pool holders数据
    def get_token_balances(self, pools, chain: str = 'Ethereum', run_date=None, block_number: int = None):
        if block_number is None:
            raise Exception('block_number 不允许为 None')
        calls = []
        abi = load_abi(self.abi, 'balanceOf')
        for row_index, pool in pools.iterrows():
            calls.append({
                'target': pool.token_address,
                'params': pool.holder_address,
            })
        print('calls: ', chain, len(calls))
        res = multi_call(calls, abi, block=block_number, chain=chain)

        token_balances = [{
            'chain': chain,
            'holder_address': call['params'],
            'token_address': call['target'],
            'date': run_date,
            'balance': item['output']} for call, item in zip(calls, res)]

        def filter_low_data(it):
            num = pydash.to_number(it['balance'])
            # print('balance ', num, bool(num) and num >= 20)
            return bool(num) and num >= 20

        token_balances = pydash.filter_(token_balances, filter_low_data)
        print('multi call res: ', len(token_balances))
        return token_balances


if __name__ == '__main__':
    job = TokenBalancesDODO()
    #
    # # 跑当天数据
    # job.load_daily_data()

    # 跑历史数据
    # job.load_history_data()

    # job.do_import_gsc_to_bigquery()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    # print('upload done')
