import datetime

from defi360.common.contract_adapter import ContractAdapter
from utils.query_bigquery import query_bigquery
from defi360.utils.multicall import load_abi, multi_call, BlockNumber
import pydash
import moment
import pandas as pd
from defi360.utils.file_cash import FileCache


class Template(ContractAdapter):
    # 自己确定数据集
    data_set = "template"
    task_name = "template"
    time_partitioning_field = "day"
    execution_time = "35 2 * * *"
    schema_name = "defi360/schema/template.json"
    abi = "defi360/contracts_adapter/abi/template.json"
    history_date = '2022-02-20'
    validate_config = []

    # 各自实现合约取数逻辑
    def get_daily_data(self, run_date: str, block_numbers: list):
        pass


if __name__ == '__main__':
    poolHolders = Template()

    # 跑历史数据
    # poolHolders.load_history_data()

    # 校验
    # poolHolders.validate()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')

