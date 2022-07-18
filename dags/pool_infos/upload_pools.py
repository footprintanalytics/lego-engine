import os

import pandas as pd
import re
import csv
import json
import moment
from google.cloud import bigquery
import datetime

from config import project_config
from utils.import_gsc_to_bigquery import get_schema
from utils.query_bigquery import query_bigquery
from datetime import timedelta
from common.pool_all_data_view_builder import AllDataViewBuilder
from common.common_dex_model import DexModel
from lending.ethereum.lending_model import LendingModel
from common.common_pool_model1 import InvestPoolModel1
from dex.ethereum.sushi.sushi import SushiDex
from dex.ethereum.uniswap.uniswap import UniswapDex
from dex.bsc.mdex.mdex import MdexDex
from dex.polygon.quickswap.quickswap import QuickswapDex
from dex.bsc.biswap.biswap import BiswapDex
from dex.ethereum.mooniswap.mooniswap import mooniswapDex
from dex.bsc.pancakeswap.pancakeswap import PancakeSwapDex
from farming.bsc.pancakeswap.pancakeswap_farming_supply import PancakeswapFarmingSupply
from farming.ethereum.truefi.truefi_farming_supply import TruefiFarmingSupply
from dex.ethereum.balancer.balancer import BalancerDex
from lending.ethereum.raricapital.raricapital_lending_liquidation import RaricapitalLendingLiquidation
from lending.bsc.venus.venus_lending_withdraw import VenusLendingWithdraw
from lending.polygon.aave.aave_lending_withdraw import AaveLendingWithdraw
from lending.ethereum.aave.aave_lending_supply import AaveLendingSupply
from lending.ethereum.truefi.truefi_lending_supply import TruefiLendingSupply
from farming.ethereum.yearn.yearn_farming_supply import YearnFarmingSupply
from dex.avalanche.traderjoe.traderjoe import TraderjoeDex
from dex.avalanche.pangolin.pangolin import PangolinDex
from lending.avalanche.aave.aave_lending_supply import AaveLendingSupply

big_query_client = bigquery.Client()


def load_to_bigquery(table_id, csv_file, schema, file_format=bigquery.SourceFormat.CSV):
    job_config = bigquery.LoadJobConfig(
        source_format=file_format,
        autodetect=False,
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_file, "rb") as source_file:
        job = big_query_client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    table = big_query_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def csv_to_json(csv_path):
    json_file = open('pool_infos.json', 'w')

    with open(csv_path, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            row['stake_token'] = row['stake_token'].split('-')
            row['stake_underlying_token'] = row['stake_underlying_token'].split('-') if row[
                'stake_underlying_token'] else []
            row['reward_token'] = row['reward_token'].split('-') if row['reward_token'] else []
            json_file.write(json.dumps(row) + '\n')


def upload_csv_pools():
    for file_name in [
        'pool_infos'
    ]:
        table_name = 'footprint_etl.{}'.format(file_name)
        csv_path = './{}.csv'.format(file_name)
        csv_to_json(csv_path)

        json_path = './{}.json'.format(file_name)
        schema = get_schema(file_name)
        print(table_name, json_path)
        load_to_bigquery(table_name, json_path, schema, bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)


def get_pool_info_sql():
    dags_folder = project_config.dags_folder
    paths = [
        os.path.join(dags_folder, 'dex/bsc'),
        os.path.join(dags_folder, 'dex/ethereum'),
        os.path.join(dags_folder, 'dex/polygon'),
        os.path.join(dags_folder, 'lending/bsc'),
        os.path.join(dags_folder, 'lending/ethereum'),
        os.path.join(dags_folder, 'lending/polygon')
    ]
    sql_list = []
    for path in paths:
        files = os.listdir(path)
        for file in files:
            sql_file_path = path + '/' + file + '/pool_infos.sql'
            if os.path.exists(sql_file_path):
                sql_file = open(sql_file_path, 'r')
                sql = sql_file.read()
                sql_list.append(sql)
                # print('\n\n-----' + sql_file_path + '------')
                # print(sql)
                sql_file.close()

    return '\n\n union all \n\n'.join(sql_list)


def get_sql_field():
    dags_folder = project_config.dags_folder
    schema_path = dags_folder + '/resources/stages/raw/schemas/pool_infos.json'
    schema_file = open(schema_path, 'r')
    schema = schema_file.read()
    schema_list = json.loads(schema)

    field_list = []
    for field in schema_list:
        field_list.append(field["name"])

    return ','.join(field_list)


def query_sql(sql: str, destination: str, write_disposition: str = 'WRITE_TRUNCATE'):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(destination=destination)
    job_config.write_disposition = write_disposition

    print('query_sql:\n', sql)
    query_job = client.query(sql, job_config=job_config)
    query_job.result()


def get_execution_date():
    now = moment.utcnow().datetime
    yesterday = now - timedelta(days=1)  # 获得前一天的时间
    execution_date = yesterday.strftime("%Y-%m-%d")
    return execution_date


def run_pool_info_sql():
    print('\n\n----- run_pool_info_sql ------')
    field_list = get_sql_field()
    pool_info_sql = get_pool_info_sql()
    create_sql = "create table if not exists `xed-project-237404.footprint_etl.pool_infos_day` CLUSTER BY project as"

    insert_sql = "insert `xed-project-237404.footprint_etl.pool_infos_day`({field_list})" \
        .format(field_list=field_list)

    pool_info_sql = "select {field_list} from ({pool_info_sql})" \
        .format(field_list=field_list, pool_info_sql=pool_info_sql)

    query_sql(pool_info_sql, 'xed-project-237404.footprint_etl.pool_infos_day')
    print(pool_info_sql)

    print("run pool_info_sql success")


def get_sql_and_table(pool: InvestPoolModel1):
    table_name_list = pool.get_daily_table_name().split('.')
    table_name_list[-1] = 'pool_infos'
    pool_infos_table = '.'.join(table_name_list)

    dags_folder = project_config.dags_folder
    sql_path_name_list = pool.source_event_sql_file.split('/')
    sql_path_name_list[-1] = 'pool_infos.sql'
    sql_file_path = '/'.join(sql_path_name_list)
    sql_file_path = os.path.join(dags_folder, sql_file_path)

    sql_file = open(sql_file_path, 'r')
    sql = sql_file.read()

    if re.search('{match_date_filter}', sql) is None:
        raise Exception('sql 中必须包含 match_date_filter')

    return {
        "sql": sql,
        "pool_infos_table": pool_infos_table
    }


def run_pool_info_history_sql(pool: InvestPoolModel1):
    dict_info = get_sql_and_table(pool)
    sql = dict_info["sql"]
    pool_infos_table = dict_info["pool_infos_table"]

    date = ' < "' + get_execution_date() + '"'
    sql = sql.format(match_date_filter=date)

    # 刷新历史数据
    query_sql(sql, pool_infos_table)
    print('run history data success')


def run_pool_info_daily_sql(pool: InvestPoolModel1, date: str = None):
    dict_info = get_sql_and_table(pool)
    sql = dict_info["sql"]
    pool_infos_table = dict_info["pool_infos_table"]

    date = ' = "' + get_execution_date() + '"' if date is None else date
    sql = sql.format(match_date_filter=date)

    # 增加昨日数据
    query_sql(sql, pool_infos_table, 'WRITE_APPEND')
    print('run daily data success')


def run_pool_info(pool: DexModel or InvestPoolModel1, date: str = None):
    try:
        pool.get_business_type('swap')
        pool = pool.get_business_type(pool.business_type['swap'])
    except Exception as e:
        print(e)

    run_pool_info_history_sql(pool)
    run_pool_info_daily_sql(pool, date)


def merge_pool_info_view():
    table_list = []

    for project in all_project:
        pool_infos_table = ''

        try:
            table_name_list = project.get_daily_table_name().split('.')
        except Exception as e:
            table_name_list = project.get_business_type(project.business_type['swap']).get_daily_table_name().split('.')

        table_name_list[-1] = 'pool_infos'
        pool_infos_table = '.'.join(table_name_list)
        table_list.append(pool_infos_table)

    table_list.append('xed-project-237404.footprint_etl.pool_infos')
    print(table_list)
    AllDataViewBuilder.build_multiply_data_view(
        table_list,
        "footprint-etl.footprint_pool_infos.pool_infos",
        """select 
        source.pool_id
        , source.protocol_id
        , d.name as project
        , source.chain
        , source.business_type
        , source.deposit_contract
        , source.withdraw_contract
        , source.lp_token
        , source.stake_token
        , source.stake_underlying_token
        , source.reward_token
        , source.name
        , source.description 
        from ({dex_source}) source
            LEFT JOIN `xed-project-237404.footprint.defi_protocol_info` d
            on source.protocol_id = d.protocol_id"""
    )


all_project = [
    SushiDex(),
    UniswapDex(),
    MdexDex(),
    QuickswapDex(),
    BiswapDex(),
    PancakeSwapDex(),
    PancakeswapFarmingSupply(),
    TruefiFarmingSupply(),
    BalancerDex(),
    RaricapitalLendingLiquidation(),
    AaveLendingWithdraw(),
    VenusLendingWithdraw(),
    TruefiLendingSupply(),
    AaveLendingSupply(),
    YearnFarmingSupply(),
    TraderjoeDex(),
    PangolinDex(),
    AaveLendingSupply()
]


def run_all_project_daily():
    for project in all_project:
        try:
            project.get_business_type('swap')
            project = project.get_business_type(project.business_type['swap'])
        except Exception as e:
            print(e)

        run_pool_info_daily_sql(project)


def airflow_steps():
    return [
        run_all_project_daily
    ]


def airflow_dag_params():
    dag_params = {
        "dag_id": "footprint_pool_infos_dag",
        "catchup": False,
        "schedule_interval": '50 1 * * *',
        "description": "footprint_pool_infos_dag",
        "default_args": {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=5),
            'start_date': datetime.datetime(2021, 8, 20)
        },
        "dagrun_timeout": datetime.timedelta(days=30)
    }
    print('dag_params', dag_params)
    return dag_params


if __name__ == '__main__':
    # upload_csv
    # upload_csv_pools()

    # run sql
    # pool1 = UniswapDex()
    # run_pool_info(pool1)
    # pool2 = MdexDex()
    # run_pool_info(pool2)
    # pool3 = QuickswapDex()
    # run_pool_info(pool3)
    # pool4 = BiswapDex()
    # run_pool_info(pool4)
    # pool6 = PancakeSwapDex()
    # run_dex_pool_info(pool6)
    # pool = RaricapitalLendingLiquidation()
    # run_pool_info(pool)
    # run_dex_pool_info(pool)
    # pool = AaveLendingWithdraw()
    # run_pool_info(pool)
    # pool = AaveLendingSupply()
    # run_pool_info(pool)

    # pool = PancakeSwapDex()
    # run_pool_info(pool)

    # pool = YearnFarmingSupply()
    # run_pool_info(pool)

    # 合成视图
    merge_pool_info_view()
