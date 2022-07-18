import datetime
import os

import moment
import pandas as pd
import pydash
from google.cloud import bigquery

from common.airflow_etl import AirFlowETL
from common.pool_all_data_view_builder import AllDataViewBuilder
from defi360.utils.file_cash import FileCache
from defi360.utils.multicall import BlockNumber
from utils.common import read_file
from utils.constant import PROJECT_PATH
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery_base, get_path_schema
from utils.query_bigquery import query_bigquery
from defi360.common.base_adapter import BaseAdapter


class BigqueryAdapter(BaseAdapter):
    # bigquery 取数 sql
    sql_path = ''

    # 从bigquery获取数据到csv
    def get_daily_data(self, run_date: str):
        if not self.sql_path or not run_date:
            raise Exception('run_date, sql_path is not exists')

        _run_date = "= '{date}'".format(date=run_date)
        sql = read_file(os.path.join(PROJECT_PATH, self.sql_path))
        sql = sql.format(run_date=_run_date)

        df = query_bigquery(sql)
        print(df.head())
        self.etl.save_data(df=df, date_str=run_date)
        print('get bigquery daily data done.')

    # 加载日数据到bigquery指定表
    def load_daily_data(self, run_date: str = None, debug: bool = False):
        if not run_date:
            run_date = self.etl.date_str

        if self.etl.is_data_file_exists(run_date):
            print('daily csv exists, pass')
            pass
        else:
            self.get_daily_data(run_date)

        if not debug:
            self.do_upload_csv_to_gsc(run_date)
            self.do_import_gsc_to_bigquery()

    # 加载历史数据到bigquery指定表
    def load_history_data(self):
        run_date = self.get_replace_sql_date()
        sql = read_file(os.path.join(PROJECT_PATH, self.sql_path))
        sql = sql.format(run_date=run_date)
        print('history sql: ', sql)

        # project用 footprint-etl-internal 不能改
        client = bigquery.Client(project='footprint-etl-internal')
        job_config = bigquery.QueryJobConfig(destination=(self.get_bigquery_history_table_name()))
        job_config.write_disposition = 'WRITE_TRUNCATE'

        if self.time_partitioning_field is not None:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,  # 兼容其他配置
                field=self.time_partitioning_field
            )

        query_job = client.query(sql, job_config=job_config)
        query_job.result()

        print('load bigquery history data done.')

    def create_data_view(self):
        AllDataViewBuilder.merge_data_table(
            table=self.get_bigquery_daily_table_name(),
            history_table=self.get_bigquery_history_table_name(),
            view_name=self.get_bigquery_table_name()
        )
