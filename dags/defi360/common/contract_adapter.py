import datetime
import os

import moment
import pandas
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
import pandas


class ContractAdapter(BaseAdapter):
    # 合约取数abi
    abi = ''
    # 合约依赖数据sql
    depend_sql_path = ''
    # 链
    chain = []

    # 实现各自的合约取数逻辑
    def get_daily_data(self, run_date: str, block_numbers: list) -> pandas.DataFrame:
        pass

    # 获取block_number
    def get_block_number(self, run_date: str):
        block_data_file_name = 'defi360/contracts_adapter/cache/{}_block_number_cache.csv'.format(run_date)
        # 大于6点则需要重刷block_number，防止遗漏昨天的bsc链数据
        is_lte_six = datetime.datetime.utcnow() < datetime.datetime.strptime(str(datetime.datetime.utcnow().date()) + ' 06:00', '%Y-%m-%d %H:%M')

        file_cache = FileCache()
        if file_cache.exist_cash_name(block_data_file_name) and is_lte_six:
            return file_cache.get_csv_data(block_data_file_name).to_dict('records')

        date = datetime.datetime.strptime(run_date, '%Y-%m-%d')
        start_date = moment.utc(date).add('days', -self.history_day - 10).format('YYYY-MM-DD')
        end_date = moment.utc(date).add('days', 10).format('YYYY-MM-DD')
        block_numbers = BlockNumber.get_bulk_block_number(start_date, end_date)

        file_cache.cash_csv_data(pd.DataFrame(block_numbers), block_data_file_name)
        return file_cache.get_csv_data(block_data_file_name).to_dict('records')
        # return block_numbers

    # 加载合约日数据到bigquery指定表
    def load_daily_data(self, run_date: str = None, block_numbers: list = None, do_load=True, debug=False):
        if not run_date:
            run_date = self.etl.date_str

        if self.etl.is_data_file_exists(run_date):
            print('contract daily csv exists, pass')
            pass
        else:
            if not block_numbers:
                block_numbers = self.get_block_number(run_date)
                block_numbers = pydash.filter_(block_numbers, {"date": run_date})
            res_df = self.get_daily_data(run_date, block_numbers)
            self.etl.save_data(res_df, date_str=run_date)

        if not debug:
            self.do_upload_csv_to_gsc(date_str=run_date)

        if not debug and do_load:
            self.do_import_gsc_to_bigquery()

    # 加载合约历史数据到bigquery指定表
    def load_history_data(self, debug=False):
        run_date = self.history_date
        execution_day = self.history_day
        block_number = self.get_block_number(run_date)

        while execution_day > 0:
            execution_day -= 1
            daily_block = pydash.filter_(block_number, {
                "date": run_date
            })
            print('load_contract_history_data: ', run_date, execution_day)

            try:
                self.load_daily_data(
                    run_date=run_date,
                    block_numbers=daily_block,
                    do_load=False,
                    debug=debug
                )
            except Exception as e:
                print('load_contract_history_data error: ', run_date, e)
                raise e

            run_date = moment.utc(
                datetime.datetime.strptime(run_date, '%Y-%m-%d')
            ).add('days', -1).strftime("%Y-%m-%d")

        # 历史数据最后import bigquery
        if not debug:
            self.do_import_gsc_to_bigquery()

    def create_data_view(self):
        AllDataViewBuilder.merge_data_table(
            table=self.get_bigquery_daily_table_name(),
            history_table=self.get_bigquery_history_table_name(),
            view_name=self.get_bigquery_table_name()
        )
