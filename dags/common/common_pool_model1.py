'''
@Project ：defi-up
@File    ：pool_daily_stats_view_builder.py
@Author  ：Nick
@Date    ：2021/8/24 3:11 下午
'''
import os
from datetime import timedelta, datetime

import pandas as pd
from google.cloud import bigquery

from common.airflow_etl import AirFlowETL
from common.pool_all_data_view_builder import AllDataViewBuilder
from common.pool_daily_stats_view_builder import DailyStatsViewBuilder
from config import project_config
from utils.sql_util import SQLUtil
from utils.import_gsc_to_bigquery import get_time_partitioning
from utils import Constant
from utils.monitor import save_monitor
from utils.query_bigquery import query_bigquery


class InvestPoolModel1:
    model_type = 'invest_model'
    project_id = 'project_id'
    client_project_id = 'client_project_id'
    project_name = None
    # 每日定时任务执行时间
    execution_time = None
    # airflow 的task name，也会是 sql table 的name
    task_name = None
    # 时间分区
    schema_name = None
    # 在此日期之前的历史数据会统一处理一次
    history_date = '2021-08-20'
    token_config = {
        "csv_file_path": '',
        "stake_token_keys": [],
        "earn_token_keys": [],
        "pool_keys": []
    }
    # 开发调试的时候，要把Transaction表先缓存到一个新表中
    pool_transaction_table_name: str = None
    # sql 路径
    source_event_sql_file: str = None

    stake_tokens = []
    earn_tokens = []
    pool_address = []
    etl: AirFlowETL = None

    inner_validate_list = []

    def do_start(self):
        rule_name = Constant.DASH_BOARD_RULE_NAME['TASK_EXECUTION']
        desc = '{task_name} rule_name is task execution'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['TASK_EXECUTION']
        item_value = 1
        result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION']
        sql = ''
        stats_date = datetime.strptime(self.etl.date_str, '%Y-%m-%d')
        save_monitor(task_name=self.task_name + '_transaction_flow',
                     execution_date=stats_date,
                     bigquery_etl_database=project_config.bigquery_etl_database,
                     table_name='',
                     rule_name=rule_name,
                     item_value=item_value,
                     result_code=result_code,
                     desc=desc,
                     desc_cn=desc_cn,
                     sql=sql,
                     field='')

    def do_end(self):
        rule_name = Constant.DASH_BOARD_RULE_NAME['TASK_EXECUTION']
        desc = '{task_name} rule_name is task execution'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['TASK_EXECUTION']
        item_value = 0
        result_code = Constant.DASH_BOARD_RESULT_CODE['REGULAR']
        sql = ''
        stats_date = datetime.strptime(self.etl.date_str, '%Y-%m-%d')
        save_monitor(task_name=self.task_name + '_transaction_flow',
                     execution_date=stats_date,
                     bigquery_etl_database=project_config.bigquery_etl_database,
                     table_name='',
                     rule_name=rule_name,
                     item_value=item_value,
                     result_code=result_code,
                     desc=desc,
                     desc_cn=desc_cn,
                     sql=sql,
                     field='')

    def init(
        self,
        project_id,
        project_name,
        task_name,
        model_type,
        schema_name,
        source_event_sql_file,
        history_date
    ):
        self.project_id = project_id
        self.project_name = project_name
        self.task_name = task_name
        self.model_type = model_type
        self.schema_name = schema_name
        self.source_event_sql_file = source_event_sql_file
        self.history_date = history_date
        self.etl = AirFlowETL(task_name=self.task_name, project_name=self.project_name)
        return self

    def __init__(self):
        self.check_not_none()
        self.parse_config_csv()
        self.etl = AirFlowETL(task_name=self.task_name, project_name=self.project_name)
        self.daily_view_builder = DailyStatsViewBuilder()

    def check_not_none(self):
        # if self.pool_transaction_table_name == '':
        #     raise Exception('pool_transaction_table_name 必须赋值')
        if self.token_config is None:
            raise Exception('token_config 必须赋值')
        if self.project_name is None:
            raise Exception('project_name 必须赋值')
        if self.task_name is None:
            raise Exception('task_name 必须赋值')
        if self.execution_time is None:
            raise Exception('execution_time 必须赋值')

    def dataframe_to_list(self, df, keys):
        if len(keys) == 0:
            return []
        ss = list(map(lambda key: df[key], keys))
        return pd.concat(ss).drop_duplicates().tolist()

    def parse_config_csv(self):
        dags_folder = project_config.dags_folder
        df = pd.read_csv(os.path.join(dags_folder, self.token_config['csv_file_path']))

        self.stake_tokens = self.dataframe_to_list(df, self.token_config['stake_token_keys'])
        self.earn_tokens = self.dataframe_to_list(df, self.token_config['earn_token_keys'])
        self.pool_address = self.dataframe_to_list(df, self.token_config['pool_keys'])

        print('check config stake_tokens ', self.stake_tokens)
        print('check config earn_tokens ', self.earn_tokens)
        print('check config pool_address ', self.pool_address)
        if self.stake_tokens is None:
            raise Exception('stake_token 不允许为空')

        if self.pool_address is None:
            raise Exception('pool_address 不允许为空')

        return df

    def build_origin_source_sql(self, match_date_filter: str):
        """
        从基础的交易表上把平台相关的交易过滤出来，查询量非常大，慎用
        :return:
        """
        return """
        with token_transfer as(
          select 
            transaction_hash,
            block_timestamp,
            from_address,
            to_address,
            token_address,
            CAST(value AS FLOAT64 ) as value
          from `bigquery-public-data.crypto_ethereum.token_transfers` where Date(block_timestamp) {match_date_filter}
          union all
          select 
            transaction_hash,
            block_timestamp,
            from_address,
            to_address,
            'eth' as token_address,
            CAST(value AS FLOAT64 ) as value
          from `bigquery-public-data.crypto_ethereum.traces` where Date(block_timestamp) {match_date_filter} and value > 0 
        )
        SELECT
            tt.transaction_hash,
            tt.block_timestamp,
            t.from_address AS op_user,
            t.to_address AS contract_address,
            t.gas,
            t.gas_price,
            tt.from_address,
            tt.to_address,
            tt.token_address,
            tt.value
        FROM
        (select * from `bigquery-public-data.crypto_ethereum.transactions` 
            where to_address {match_pool_address} and Date(block_timestamp) {match_date_filter} and receipt_status = 1)t
        LEFT JOIN 
        token_transfer as tt
        ON t.hash = tt.transaction_hash
        """.format(
            match_pool_address=SQLUtil.build_match_tokens(self.pool_address),
            match_date_filter=match_date_filter
        )

    def get_source_table_name(self):
        return "{}.{}".format(self.project_id, self.pool_transaction_table_name)

    def get_daily_table_name(self):
        return "{}.{}.{}".format(
            self.project_id,
            project_config.bigquery_etl_database,
            self.task_name)

    def get_history_table_name(self):
        return self.get_daily_table_name() + '_before_' + self.history_date

    def create_source_table(self):
        """
        将交易表转成到一个新表中，减少查询量
        :return:
        """
        client = bigquery.Client()
        table_id = self.get_source_table_name()
        job_config = bigquery.QueryJobConfig(destination=table_id)

        source_sql = self.build_source_sql()
        # Start the query, passing in the extra configuration.
        query_job = client.query(source_sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

        print("Query results loaded to the table {}".format(table_id))

    def build_cache_source_sql(self):
        return """
        SELECT 
            *
        FROM `{source_table_name}`
        WHERE transaction_hash IS NOT NULL
        """.format(source_table_name=self.get_source_table_name())

    def build_source_sql(self):
        if self.pool_transaction_table_name is None:
            return self.build_origin_source_sql()
        else:
            return self.build_cache_source_sql()

    def build_classify_function(self):
        """
        所有的分类 method 都是相对于 from_address 来描述的，
        用 from_address + token类型 + to_address 来定义一个转账的类型
        :return:
        """
        profit_match = ""
        if len(self.earn_tokens) > 0:
            profit_match = "WHEN op_user = to_address AND token_address {match_profit_tokens} THEN 'profit'" \
                .format(match_profit_tokens=SQLUtil.build_match_tokens(self.earn_tokens), )
        return """
        CASE
            WHEN op_user = from_address AND token_address {match_stake_tokens} AND to_address {match_contract} THEN 'deposit'
            WHEN op_user = to_address AND token_address {match_stake_tokens} AND from_address {match_contract} THEN 'withdraw'
            {profit_match}
            ELSE 'UnKnow'
        END AS operation,
        """.format(
            profit_match=profit_match,
            match_stake_tokens=SQLUtil.build_match_tokens(self.stake_tokens),
            match_contract=SQLUtil.build_match_tokens(self.pool_address),
        )

    def build_classify_transaction_sql(self, source: str):
        """
        先用原生 SQL 实现分类，后面处理复杂逻辑要用 JS UDF
        :return:
        """
        return """
            WITH source_table AS (
                {source}
            ),
            -- 识别不同的操作
            transactions_op AS (
                SELECT 
                *,
                {classify_function}
                FROM source_table
            ),
            -- 过滤未知的操作
            transactions_filter AS (
                SELECT 
                *
                FROM transactions_op
                WHERE operation != 'UnKnow' 
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
                FROM transactions_filter s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON s.token_address = LOWER(t.contract_address)
            )
            SELECT * FROM transactions_token
            """.format(
            source=source,
            classify_function=self.build_classify_function()
        )

    def build_history_data_sql(self):
        match_date_filter = "< '{}'".format(self.history_date)
        source_sql = self.build_origin_source_sql(match_date_filter)
        query_string = self.build_classify_transaction_sql(source=source_sql)
        return query_string

    def parse_history_data(self):
        """
        在 history_date 之前的历史数据会统一处理一次
        :return:
        """
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig(destination=(self.get_history_table_name()))
        job_config.write_disposition = 'WRITE_TRUNCATE'
        if self.schema_name is not None:
            job_config.time_partitioning = get_time_partitioning(self.schema_name)
        source_sql = self.build_history_data_sql()
        print(source_sql)
        # Start the query, passing in the extra configuration.

        query_job = client.query(source_sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

        print("Query results loaded to the table {}".format(self.get_history_table_name()))
        # 校验历史数据
        # self.validate(self.get_history_table_name())

    def build_daily_data_sql(self):
        match_date_filter = "= '{}'".format(self.etl.date_str)
        source_sql = self.build_origin_source_sql(match_date_filter)
        query_string = self.build_classify_transaction_sql(source=source_sql)
        return query_string

    def parse_daily_data(self):
        # if self.etl.is_data_file_exists():
        #     print('skip parse_daily_data')
        #     return
        print('parse_daily_data')
        daily_sql = self.build_daily_data_sql()
        bqclient = bigquery.Client(project=self.project_id)
        df = bqclient.query(daily_sql).result().to_dataframe(create_bqstorage_client=False)
        print(df.head())
        self.etl.save_data(df)
        print('parse_daily_data done')

    def get_defi_protocol_info(self):
        sql = """
        select c.protocol_id,c.project,d.chain from {table} c
        left join `xed-project-237404.footprint_etl.defi_protocol_info` d
        on c.protocol_id = d.protocol_id
        group by c.protocol_id,c.project,d.chain
        """.format(table=self.get_daily_table_name()+'_all')
        print(sql)
        df = query_bigquery(sql, project_id=self.project_id)
        return df.to_dict(orient='records')

    def do_import_gsc_to_bigquery(self):
        self.etl.do_import_gsc_to_bigquery()

    def run_daily_job(self, date_str=None):
        if date_str is not None:
            print('reset execution_date ', date_str)
            self.etl.date_str = date_str
        self.parse_daily_data()
        self.etl.do_upload_csv_to_gsc()
        self.do_import_gsc_to_bigquery()
        # self.validate(self.get_daily_table_name())
        # self.external_validate(date_str)

    def external_validate(self, date_str: str = None):
        pass

    # 基础校验
    def validate(self, validate_table: str = None):
        pass

    def create_all_data_view(self):
        AllDataViewBuilder.build_all_data_view(
            transactions_table=self.get_daily_table_name(),
            transactions_history_table=self.get_history_table_name(),
            history_date=self.history_date,
            date_column='block_timestamp'
        )

    def create_pool_daily_view(self):
        DailyStatsViewBuilder.build_pool_daily_stats_view(self.get_daily_table_name())

    def airflow_steps(self):
        return [
            self.do_start,
            self.parse_daily_data,
            self.etl.do_upload_csv_to_gsc,
            self.do_import_gsc_to_bigquery,
            self.validate,
            self.do_end
        ]

    def airflow_dag_params(self):
        dag_params = {
            "dag_id": "footprint_{}_{}_dag".format(self.model_type, self.task_name),
            "catchup": False,
            "schedule_interval": self.execution_time,
            "description": "{}_dag".format(self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 5,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 8, 20)
            },
            "dagrun_timeout": timedelta(days=30),
            "tags": ['m1_transaction']
        }
        print('dag_params', dag_params)
        return dag_params
