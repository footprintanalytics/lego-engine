'''
@Project ：defi-up
@File    ：common_pool_model3.py
@Author  ：Nick
@Date    ：2021/8/24 3:11 下午
'''
import os
from datetime import datetime

from google.cloud import bigquery

from common.airflow_etl import AirFlowETL
from common.common_pool_model1 import InvestPoolModel1
from common.pool_daily_stats_view_builder import DailyStatsViewBuilder
from config import project_config
from utils.common import read_file
from utils.gcs_util import GCSUtil
from utils.constant import PROJECT_PATH


class InvestPoolModel3(InvestPoolModel1):
    source_event_sql_file: str = None

    def __init__(self):
        super().__init__()
        self.check_not_none()
        self.etl = AirFlowETL(task_name=self.task_name, project_name=self.project_name)
        self.daily_view_builder = DailyStatsViewBuilder()

    def parse_config_csv(self):
        pass

    def check_not_none(self):
        super().check_not_none()
        if self.source_event_sql_file is None:
            raise Exception('source_event_sql_file 必须赋值')

    def build_origin_source_sql(self, match_date_filter: str):
        """
        从时间表上把平台相关的交易过滤出来
        :return:
        """
        dags_folder = PROJECT_PATH
        sql_path = os.path.join(dags_folder, self.source_event_sql_file)
        sql = read_file(sql_path)
        sql = sql.format(match_date_filter=match_date_filter)
        print('build_origin_source_sql query_string', sql)
        return sql

    def build_classify_transaction_sql(self, source: str):
        """
        先用原生 SQL 实现分类，后面处理复杂逻辑要用 JS UDF
        :return:
        """
        return """
            WITH source_table AS (
                {source}
            ),
            -- 连 tokoen 表
            transactions_token AS(
                SELECT
                s.transaction_hash,
                s.block_timestamp,
                s.op_user,
                s.contract_address,
                s.gas,
                s.gas_price,
                s.from_address,
                s.to_address,
                s.token_address,
                t.symbol AS token_symbol,
                s.operation,
                value / POW(10, t.decimals) AS value,
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON s.token_address = t.contract_address
            )
            SELECT * FROM transactions_token
            """.format(source=source)

    @property
    def table_prefix(self):
        return '.'.join(self.get_daily_table_name().split('.')[:2])

    def delete_history_data(self, is_test=True): # note: 该功能仅供改dex & lending平台使用, 其他用途使用时请注意测试!!!!
        """
        is_test: 是否开启测试模式, 测试模式下, 不会做任何操作, 只会输出log, 用于调试. 默认调试模式
        """
        if is_test:
            print('开启测试模式')
        self._delete_history_gcs_data(is_test)
        self._delete_history_bq_data(is_test)

    def _delete_history_bq_data(self, is_test: bool = True):
        bqclient = bigquery.Client(project=self.project_id)
        sql = f"""
SELECT
  table_name
FROM
  `{self.table_prefix}.INFORMATION_SCHEMA.TABLES`
WHERE
  table_type="BASE TABLE"
  AND table_name IN (
    '{self.get_daily_table_name().split('.')[-1]}',
    '{self.get_history_table_name().split('.')[-1]}')
"""
        df = bqclient.query(sql).result().to_dataframe(create_bqstorage_client=False)
        for name in df['table_name']:
            table_id = f'{self.table_prefix}.{name}'
            remove_sql = f'DELETE FROM `{table_id}` WHERE true'
            if not is_test:
                bqclient.query(remove_sql).result()
            print(f"Deleted table data '{table_id}'.")

    def _delete_history_gcs_data(self, is_test: bool = True):
        gcs_util = GCSUtil(project_config.bigquery_bucket_name)
        blobs = gcs_util.list_dir(
            '{gsc_folder}/{task_name}'.format(gsc_folder=self.task_name, task_name=self.task_name), return_blob=True)
        dt = datetime.now().isoformat().split('.')[0]
        for blob in blobs:
            destination_blob_name = '_bkg/{gcs_floder}_{datetime}/{file_name}'.format(
                gcs_floder=self.task_name,
                datetime=dt,
                file_name=blob.name.split('/')[-1]
            )
            if not is_test:
                gcs_util.move_file(
                    blob.name,
                    destination_blob_name=destination_blob_name,
                    source_blob=blob
                )
            else:
                print("Blob {} has been renamed to {}".format(blob.name, destination_blob_name))

    def get_history_table_name(self):
        return self.get_daily_table_name() + '_history'

