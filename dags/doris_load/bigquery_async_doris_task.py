import pymysql
import random
import pydash
from utils.constant import BIGQUERY_EXPORT_DATA_TASK_STATUS
from doris_load.rename_doris_table import rename
from config import project_config
from models import BigqueryImportDorisTask, BigqueryExportDataTask
from doris_load.check_bigquery_doris_row_count import check_bigquery_doris_row_count
from doris_load.rename_doris_table import check_rename_export_data_task

max_available_tasks = 5

table_config = [
]
table_sequence = [
    'polygon_transactions_sequence'
]

def bigquery_async_doris_task():
    static_task_num = random.randint(0, 100000)
    print(static_task_num)
    handle_bigquery_import_doris_task(static_task_num)


def mysql_conn(doris_catalog: str):
    table_level = doris_catalog.split('.')
    database = table_level[0]
    # 打开数据库连接
    conn = pymysql.connect(host=project_config.doris_host,
                           port=project_config.doris_port,
                           user=project_config.doris_user,
                           password=project_config.doris_password,
                           database=database)
    return conn


def handle_bigquery_import_doris_task(static_task_num):
    handle_update_task_status()
    loading_task_num = doris_loading_task_num()
    available_tasks = max_available_tasks - loading_task_num
    if available_tasks > 0:
        multi_doris_load(available_tasks, static_task_num)


def multi_doris_load(available_tasks, static_task_num):
    query = {
        'state': {'$in': [BIGQUERY_EXPORT_DATA_TASK_STATUS['PENDING']]},
        'doris_catalog': {'$ne': ''}
    }
    if len(table_config) > 0:
        query['bigquery_catalog'] = {'$in': table_config}
    # 需要做排序操作
    waiting_tasks = list(BigqueryImportDorisTask.find(query).limit(available_tasks))
    print(waiting_tasks)
    for task in waiting_tasks:
        print(task)
        try:
            doris_load(task, static_task_num)
        except Exception as e:
            print('multi_doris_load fail ***********')
            print(e)


def handle_update_task_status():
    loading_query = {
        'state': {'$in': [BIGQUERY_EXPORT_DATA_TASK_STATUS['ETL'], BIGQUERY_EXPORT_DATA_TASK_STATUS['LOADING']]}
    }
    loading_tasks = list(BigqueryImportDorisTask.find(loading_query))
    if len(loading_tasks) <= 0:
        return
    for loading_task in loading_tasks:
        handle_check_and_update(loading_task)


def get_export_data_start_date(export_data_task_id):
    task = BigqueryExportDataTask.find_by_id(_id=export_data_task_id, projection={'start_date': 1})
    return pydash.get(task,'start_date')


def handle_check_and_update(task):
    export_data_task_id = task.get('export_data_task_id')
    bigquery_catalog = task.get('bigquery_catalog')
    label = task.get('label')
    doris_catalog = task.get('doris_catalog')
    if doris_catalog is None:
        return
    conn = mysql_conn(doris_catalog)
    label_state = get_label_state(label, conn)
    if label_state.get('state') is None:
        return
    # 状态有变则更新
    elif label_state.get('state') != task.get('state') and label_state.get('state') != BIGQUERY_EXPORT_DATA_TASK_STATUS['PENDING']:
        BigqueryImportDorisTask.find_one_and_update({'_id': task['_id']}, set_dict={'state': label_state.get('state')})
        if label_state.get('state') == BIGQUERY_EXPORT_DATA_TASK_STATUS['FINISHED']:
            rename(export_data_task_id, bigquery_catalog)
            if check_rename_export_data_task(export_data_task_id, bigquery_catalog, True):
                start_date = get_export_data_start_date(export_data_task_id)
                if start_date is not None:
                    check_bigquery_doris_row_count(bigquery_catalog, start_date)
    conn.close()


def doris_load(task, static_task_num):
    query = {'_id': task['_id']}
    BigqueryImportDorisTask.find_one_and_update(query, set_dict={'state': BIGQUERY_EXPORT_DATA_TASK_STATUS['WAIT_LOADING']})

    conn = mysql_conn(task['doris_catalog'])
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    load_sql = json_load_sql(task, static_task_num)
    cursor.execute(load_sql)
    cursor.close()
    label = get_label(task, static_task_num)
    # 记录状态
    loading_state = get_label_state(label, conn)
    if loading_state['state'] is None or loading_state['state'] == BIGQUERY_EXPORT_DATA_TASK_STATUS['PENDING'] or \
            loading_state['state'] == BIGQUERY_EXPORT_DATA_TASK_STATUS['ETL']:
        loading_state['state'] = BIGQUERY_EXPORT_DATA_TASK_STATUS['LOADING']
    BigqueryImportDorisTask.find_one_and_update(query, set_dict={
        'label': label,
        'state': loading_state['state'] if loading_state['state'] else BIGQUERY_EXPORT_DATA_TASK_STATUS['LOADING'],
        'error_msg': loading_state['error_msg'],
        'log_url': loading_state['log_url'],
        'load_sql': load_sql
    })
    conn.close()
    return label


def get_label(task: object, static_task_num):
    table_level = task['doris_catalog'].split('.')
    table = table_level[1]
    return f"{table}_{task['_id']}_json_{static_task_num}"


def json_load_sql(task, static_task_num):
    export_data_task_id = task['export_data_task_id']
    export_data_task = BigqueryExportDataTask.find_by_id(export_data_task_id)
    if not export_data_task:
        print('找不到对应的导出任务**************', export_data_task_id)
        return

    table_level = task['doris_catalog'].split('.')
    table = table_level[1]
    data_infile = append_data_infile(task['gcs_urls'], export_data_task['gcs_bucket_name'])
    sql = f"""
            LOAD LABEL {task['doris_catalog']}_{task['_id']}_json_{static_task_num} (
              DATA INFILE(
                {data_infile}
              ) INTO TABLE {table} COLUMNS TERMINATED BY "," FORMAT AS "json"
            ) WITH S3 (
              "AWS_ENDPOINT" = "https://storage.googleapis.com",
              "AWS_ACCESS_KEY" = "{project_config.doris_access_key}",
              "AWS_SECRET_KEY" = "{project_config.doris_secret_key}",
              "AWS_REGION" = "us-east4"
            ) PROPERTIES (
              "timeout" = "7200"
            );
            """
    if table == 'polygon_transactions_temp':
        sql = f"""
            LOAD LABEL {task['doris_catalog']}_{task['_id']}_json_{static_task_num} (
              DATA INFILE(
                {data_infile}
              ) INTO TABLE {table} COLUMNS TERMINATED BY "," FORMAT AS "json" 
              order by block_date
            ) WITH S3 (
              "AWS_ENDPOINT" = "https://storage.googleapis.com",
              "AWS_ACCESS_KEY" = "{project_config.doris_access_key}",
              "AWS_SECRET_KEY" = "{project_config.doris_secret_key}",
              "AWS_REGION" = "us-east4"
            ) PROPERTIES (
              "timeout" = "7200"
            );
            """

    print(sql)
    return sql


def append_data_infile(gcs_urls, gcs_bucket_name):
    gcs_url_join = ''
    for index in range(0, len(gcs_urls)):
        gcs_url = gcs_urls[index]
        gcs_url = f'"s3://{gcs_bucket_name}/' + gcs_url + '"'
        gcs_url_join = gcs_url if index == 0 else gcs_url + ',' + gcs_url_join

    return gcs_url_join


def doris_loading_task_num():
    query = {
        'state': {'$in': [BIGQUERY_EXPORT_DATA_TASK_STATUS['ETL'], BIGQUERY_EXPORT_DATA_TASK_STATUS['LOADING']]}
    }
    return BigqueryImportDorisTask.count(query=query)


def get_label_state(label: str, conn: pymysql.Connection):
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    sql = f"""show load where label = '{label}'  order by CreateTime desc  limit 1"""
    cursor.execute(sql)
    state = None
    error_msg = None
    log_url = None
    for r in cursor.fetchall():
        # label 状态第三个字段
        state = r[2]
        error_msg = r[7]
        log_url = r[13]
    cursor.close()
    return {'state': state, 'error_msg': error_msg, 'log_url': log_url}


if __name__ == '__main__':
    bigquery_async_doris_task()
