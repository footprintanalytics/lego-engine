import pymysql
import pydash
from utils.query_bigquery import query_bigquery
from models import TableInfo, BigqueryMappingDorisTableInfo
from config import project_config
import pandas
from utils.date_util import DateUtil
import os
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from datetime import datetime

WHITE_STRATEGY = [
    {'strategy': 'bq_min_date', 'table_name': 'polygon_transactions'},
    {'strategy': 'bq_min_date', 'table_name': 'polygon_token_transfers'},
    {'strategy': 'bq_min_date', 'table_name': 'bsc_token_transfers'},
    {'strategy': 'doris_min_date', 'table_name': 'solana_token_transfers'},
    {'strategy': 'doris_min_date', 'table_name': 'ud_solana_token_transfers'},
    {'strategy': 'acceptable_diff_count', 'table_name': 'arbitrum_transactions', 'acceptable_diff_count': 3}
]


def get_min_date_bigquery(bigquery_table_name, select_key):
    bigquery_result = query_bigquery(
        query_string=f"""select min({select_key}) as min_date from {bigquery_table_name}""")
    bigquery_result = bigquery_result.to_dict('records')
    return bigquery_result[0]['min_date']


def get_min_date_doris(doris_table_name, select_key):
    doris_result = doris_query(f"""select min({select_key}) as min_date from {doris_table_name}""")
    return doris_result


def get_check_count_result(table_name, count):
    match_strategy = pydash.find(WHITE_STRATEGY, {"table_name": table_name})
    strategy = pydash.get(match_strategy, 'strategy')
    diff_count = 0
    if strategy == 'acceptable_diff_count':
        diff_count = pydash.get(match_strategy, 'acceptable_diff_count', 0)
    return abs(count) <= diff_count


def get_where_condition_by_strategy(bigquery_table_name, doris_table_name, select_key):
    arr = bigquery_table_name.split('.')
    table_name = arr[2]
    match_strategy = pydash.find(WHITE_STRATEGY, {"table_name": table_name})
    strategy = pydash.get(match_strategy, 'strategy')
    condition = ''
    if strategy == 'bq_min_date':
        min_date = get_min_date_bigquery(bigquery_table_name, select_key)
        condition = f"""and timestamp({select_key}) >= timestamp('{min_date}')"""
    elif strategy == 'doris_min_date':
        min_date = get_min_date_doris(doris_table_name, select_key)
        condition = f"""and timestamp({select_key}) >= timestamp('{min_date}')"""
    return condition


def check_bigquery_doris_row_count(table_path, check_date):
    current_date = DateUtil.utc_current()
    format_datetime = current_date.strftime('%Y-%m-%d %H:%M:%S')
    format_date = current_date.strftime('%Y-%m-%d')
    select_key = get_table_select_key(table_path)
    table_name = table_path.replace(":", "_").replace(".", "_")
    print('select_key:', select_key)

    result_list = []
    # for table_info in table_infos:
    #     table_path = table_info['table_path']
    try:
        bigquery_mapping_doris_table_info = get_bigquery_mapping_doris_table_info(table_path)
        if not bigquery_mapping_doris_table_info:
            return
        item = diff_bigquery_vs_doris_row_count(bigquery_mapping_doris_table_info, check_date, select_key)
        result_list.append(item)
    except Exception as e:
        print('扫描数据异常', table_path, e)
    df = pandas.DataFrame(result_list,
                          columns=['date', 'domain', 'database_schema', 'category', 'table', 'test_name', 'status',
                                   'failures', 'message'])
    csv_file = get_csv_file_name(format_date, table_name)
    df.to_csv(csv_file, index=False, header=True)

    destination_file_path = f'monitor/data_quality/sync_doris/{table_name}/{format_date}/sync_doris_{table_name}_{format_datetime}.csv'

    upload_csv_to_gsc(csv_file, destination_file_path, project_config.sync_doris_bucket)


def get_csv_file_name(format_date, table_name):
    dags_folder = project_config.dags_folder
    return os.path.join(dags_folder, '../data/sync_doris/{}_monitor_{}.csv'.format(table_name, format_date))


def diff_bigquery_vs_doris_row_count(bigquery_mapping_doris_table_info, check_date, select_key):
    bigquery_table_name = bigquery_mapping_doris_table_info['bigquery_table_name'].replace(':', '.')
    arr = bigquery_table_name.split('.')
    project_name = arr[0]
    dataset_name = arr[1]
    table_name = arr[2]
    doris_table_name = bigquery_mapping_doris_table_info['doris_table_name']
    # {get_where_condition_by_strategy(bigquery_table_name, doris_table_name, select_key)}
    where_condition = f'where timestamp({select_key}) <= timestamp("{check_date}") ' if select_key else ''

    bigquery_sql = f'select count(*) as count from {bigquery_table_name} {where_condition}'
    doris_sql = f'select count(*) as count from {doris_table_name} {where_condition}'
    bigquery_result = query_bigquery(bigquery_sql)

    bigquery_result = bigquery_result.to_dict('records')

    doris_result = doris_query(doris_sql)

    diff_count = bigquery_result[0]['count'] - doris_result

    date_time = DateUtil.utc_current().strftime('%Y-%m-%d %H:%M:%S')

    item = {
        'date': date_time,
        'domain': '',
        'database_schema': f'{project_name}.{dataset_name}',
        'category': 'sync_doris',
        'table': table_name,
        'test_name': 'bg_doric_same_count',
        'status': 'pass' if get_check_count_result(table_name,diff_count) else 'fail',
        'failures': diff_count,
        'message': ''
    }

    return item


def doris_query(sql: str):
    try:
        conn = pymysql.connect(host=project_config.doris_host,
                               port=project_config.doris_port,
                               user=project_config.doris_user,
                               password=project_config.doris_password,
                               database='example_db',
                               read_timeout=400)
        print(sql)
        # 使用cursor()方法获取操作游标
        cursor = conn.cursor()
        result = 0
        cursor.execute(sql)
        for r in cursor:
            result = r[0]
    except Exception as e:
        print('doris_query ************* fail')
        print(e)
    cursor.close()
    conn.close()

    return result


def get_table_select_key(table_path):
    table_info = TableInfo.find_one({'table_path': table_path})
    return table_info['select_key']
    # table_infos = TableInfo.find_list(query={'status': {'$ne': 'disable'}})
    # return table_infos


def get_bigquery_mapping_doris_table_info(table_path):
    bigquery_mapping_doris_table_info = BigqueryMappingDorisTableInfo.find_one({'bigquery_table_name': table_path})
    return bigquery_mapping_doris_table_info


if __name__ == '__main__':
    check_bigquery_doris_row_count('gaia-data:gaia.solana_token_transfers', datetime.now())
