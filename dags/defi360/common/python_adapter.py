import os

import pandas as pd

from common.airflow_etl import ensure_dir
from config import project_config
from defi360.common.base_adapter import BaseAdapter
from utils.common import read_file
from utils.constant import PROJECT_PATH
from utils.date_util import DateUtil
from utils.gql.gql_task_execution_flag import GQLTaskExecutionFlag
from utils.import_gsc_to_bigquery import load_csv_to_bigquery, get_path_schema
from utils.query_bigquery import query_bigquery


class PythonAdapter(BaseAdapter):
    # bigquery 取数 sql
    sql_path = ''
    category = ''

    def save_data_to_csv(self, df: pd.DataFrame, bigquery_data_file_path: str):
        ensure_dir(bigquery_data_file_path)
        print('save csv file to ', bigquery_data_file_path)
        df.to_csv(bigquery_data_file_path, index=False)
        return bigquery_data_file_path

    def build_file_path(self, file_path):
        dags_folder = project_config.dags_folder
        return os.path.join(dags_folder, file_path.format(self.task_name.lower()))

    def get_bigquery_data_file_path(self):
        return self.build_file_path('defi360/python_adapter/data/bigquery_data/{}_data.csv')

    def get_transfer_data_file_path(self):
        return self.build_file_path('defi360/python_adapter/data/transfer_data/{}_data.csv')

    # 从bigquery获取数据到csv
    def get_bigquery_data(self):
        if not self.sql_path:
            raise Exception('sql_path is not exists')

        sql = read_file(os.path.join(PROJECT_PATH, self.sql_path))

        df = query_bigquery(sql)
        print(df.head())

        print('get bigquery daily data done.')
        return df

    def python_etl(self, df: pd.DataFrame):
        raise Exception('not implement!')

    def load_csv_data_to_bigquery(self):
        table_name = self.get_bigquery_table_name()
        schema = get_path_schema(self.schema_name)
        file_path = self.get_transfer_data_file_path()
        load_csv_to_bigquery(table_name, file_path, schema)

    # 从bigquery获取数据到csv
    def run_etl(self, debug: bool = False):
        df = self.get_bigquery_data()
        self.save_data_to_csv(df, self.get_bigquery_data_file_path())

        new_df = self.python_etl(df)
        self.save_data_to_csv(new_df, self.get_transfer_data_file_path())
        # import csv to bg
        if not debug:
            self.load_csv_data_to_bigquery()

    def airflow_steps(self):
        return [
            self.run_etl
        ]

    def insertTaskExecutionFlag(self):
        if not self.category:
            return
        params = {
            'task_name': self.task_name,
            'association_table': '',
            'category': self.category,
            'data_last_time': self.etl.date_str,  # 数据最新的时间
            'last_updated_by': 'system',  # 记录最后由谁更新的
            'last_updated_at': DateUtil.utc_current()  # 记录最后更新的时间
        }
        GQLTaskExecutionFlag().insertTaskExecutionFlag(params)
