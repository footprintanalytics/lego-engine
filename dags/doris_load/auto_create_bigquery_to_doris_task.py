from models import BigqueryExportDataTask, TableInfo, GscTableSource, BigqueryImportDorisTask, BigqueryMappingDorisTableInfo
from utils.date_util import DateUtil
from utils.query_bigquery import query_bigquery
from doris_load.create_doris_temp_table import create_doris_temp_table
import datetime
import pydash
import random
from google.cloud import bigquery, storage


def auto_create_bigquery_to_doris_task():
    check_and_update_loading_export_data_task()
    export_data_tasks = get_all_pending_export_data_task()
    for export_data_task in export_data_tasks:
        try:
            handle_export_data_task(export_data_task)
        except Exception as e:
            print('handle_export_data_task fail *********')
            print(e)


def handle_export_data_task(export_data_task):
    async_min_time = export_data_task['async_min_time']
    current_date = DateUtil.utc_current()
    today_start_date = DateUtil.utc_start_of_date().strftime('%Y-%m-%d')
    async_min_date = datetime.datetime.strptime(f'{today_start_date} {async_min_time}:00.00+00:00',
                                                '%Y-%m-%d %H:%M:%S.%f%z')
    if current_date < async_min_date:
        print('export data task 不满足今天的导出时间')
        return

    # 执行导数据的操作
    export_table_data_to_gcs(export_data_task)


def export_table_data_to_gcs(export_data_task):
    static_task_num = random.randint(0, 100000)
    table_path = export_data_task['table_path']
    strategy = pydash.get(export_data_task, 'strategy', 'upsert')
    table_query = {
        'table_path': table_path,
        'gcs_bucket_name': export_data_task['gcs_bucket_name']
    }
    table_info = TableInfo.find_one(table_query)

    export_data_task_id = export_data_task['_id']
    export_data_task_query = {
        '_id': export_data_task_id
    }
    if not table_info:
        print(f'{str(export_data_task_id)}export_data_task table_info 不存在!!!!!')
        return

    # delete_insert 策略需要创建临时表temp
    if strategy == 'delete_insert':
        create_doris_temp_table(table_path)

    gcs_root_path = table_info['gcs_root_path']
    gcs_bucket_name = table_info['gcs_bucket_name']
    export_data_start_date_str = export_data_task['export_data_start_date'].strftime('%Y-%m-%d')

    export_data_task['export_data_end_date'] = DateUtil.utc_x_hours_after(24 * 1, export_data_task['export_data_end_date'])
    export_data_end_date_str = export_data_task['export_data_end_date'].strftime('%Y-%m-%d')
    project = table_info['project']
    dataset = table_info['dataset']
    table_name = table_info['table_name']
    select_key = table_info['select_key']

    table_name = f'{project}.{dataset}.{table_name}'

    check_result = check_bigquery_has_select_day_data(table_name, select_key, export_data_start_date_str, export_data_end_date_str)

    # 如果日更数据，当天没产生数据，不执行
    if not check_result['has_data']:
        BigqueryExportDataTask.find_one_and_update(export_data_task_query, set_dict={'state': 'CANCELLED'})
        print(f'{table_name} ******* {export_data_start_date_str} ******** {export_data_end_date_str} 数据为空，暂时不执行数据同步')
        return

    gcs_dir = f'{export_data_start_date_str}~{export_data_end_date_str}_{static_task_num}'

    uri = f'gs://{gcs_bucket_name}{gcs_root_path}{gcs_dir}/*.json'
    query_condition = '' if not select_key else f'where {select_key} >= "{export_data_start_date_str}" and {select_key} <= "{export_data_end_date_str}"'
    query_sql = f'select * from {table_name} {query_condition}'
    sql = f"""EXPORT DATA
  OPTIONS(
    uri ='{uri}',
    format='JSON',
    overwrite=true)
  AS {query_sql}"""

    print(sql)

    BigqueryExportDataTask.find_one_and_update(export_data_task_query, set_dict={'state': 'EXPORTING', 'uri': uri, 'load_sql': sql})
    client = bigquery.Client(project='footprint-etl-internal')
    result = client.query(sql)
    print('数据错误结果为:')
    print(result.error_result)
    if not result.error_result:
        BigqueryExportDataTask.find_one_and_update(export_data_task_query,
                                                   set_dict={'state': 'EXPORT_FINISH', 'uri': uri, 'load_sql': sql, 'job_id': result.job_id, 'row_count': check_result['count'], 'start_date': DateUtil.utc_current()})
    else:
        BigqueryExportDataTask.find_one_and_update(export_data_task_query, set_dict={'state': 'EXPORT_FAIL', 'uri': uri, 'load_sql': sql, 'job_id': result.job_id, 'row_count': check_result['count']})


def check_bigquery_has_select_day_data(table_name, select_key, export_data_start_date_str, export_data_end_date_str):
    query_condition = '' if not select_key else f'where {select_key} >= "{export_data_start_date_str}" and {select_key} < "{export_data_end_date_str}"'
    sql = f'select count(*) as count from {table_name} {query_condition}'
    result = query_bigquery(sql)
    result = result.to_dict('records')
    return {
        'count': result[0]['count'],
        'has_data': True if result[0]['count'] > 0 else False
    }


def get_bigquery_gcs_object_list(export_data_task):
    gcs_bucket_name = export_data_task['gcs_bucket_name']
    uri = export_data_task['uri']
    prefix = uri.replace('/*.json', '').replace(f'gs://{gcs_bucket_name}/', '')

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(gcs_bucket_name, prefix=prefix, delimiter='')
    return blobs


def create_bigquery_gcs_source(export_data_task):
    blobs = get_bigquery_gcs_object_list(export_data_task)
    # 保存一份原始数据
    for blob in blobs:
        insert_gcs_source_data(blob.name, str(blob.size), str(export_data_task['_id']), export_data_task['table_path'])


def insert_gcs_source_data(name, size, export_data_task_id, table_path):
    table_info = TableInfo.find_one({'table_path': table_path})

    name_arr = name.split('/')
    file_name = name_arr[len(name_arr) - 1]
    table_name = table_info['table_name']
    dataset_name = table_info['dataset']
    project_name = table_info['project']
    query = {
        'gcs_url': name,
        'export_data_task_id': export_data_task_id
    }

    update = {
        'table_key': f'{project_name}-{dataset_name}-{table_name}',
        'file_size': float(size) / (1024 * 1024),
        'file_name': file_name,
        'table_name': table_name,
        'dataset_name': dataset_name,
        'project_name': project_name
    }
    GscTableSource.find_one_and_update(query, set_dict=update, upsert=True)


def handle_create_doris_data_config(export_data_task):
    export_data_task_id = str(export_data_task['_id'])
    export_data_task_is_temp = pydash.get(export_data_task, 'is_temp', False)
    all_table_keys = GscTableSource.distinct('table_key', {'export_data_task_id': export_data_task_id})

    for table_key in all_table_keys:
        table_sources = list(GscTableSource.find({'table_key': table_key, 'export_data_task_id': export_data_task_id}).sort('file_name', 1))
        first_table_source = table_sources[0]
        project_name = first_table_source['project_name']
        dataset_name = first_table_source['dataset_name']
        table_name = first_table_source['table_name']

        bigquery_table_name = f'{project_name}:{dataset_name}.{table_name}'

        mapping_result = BigqueryMappingDorisTableInfo.find_one({'bigquery_table_name': bigquery_table_name})

        error_message = ''
        if not mapping_result:
            print(f'{bigquery_table_name} ***** 找不到表doris_bigquery_mapping 配置文件')
            continue

        table_source_group_arr = divide_gsc_table_source_group(table_sources)
        doris_catalog = mapping_result['doris_table_name']

        for table_source_group in table_source_group_arr:
            file_group = []
            group_file_size = 0
            for table_source in table_source_group:
                group_file_size = group_file_size + pydash.get(table_source, 'file_size', 0)
                gcs_url = table_source['gcs_url']
                file_group.append(gcs_url)
            query = {
                'export_data_task_id': export_data_task_id,
                'gcs_url': pydash.join(file_group, ',')
            }
            source_import_result = BigqueryImportDorisTask.find_one(query)
            update = {
                'label': '',
                'gcs_urls': file_group,
                'bigquery_catalog': bigquery_table_name,
                'doris_catalog': doris_catalog if not export_data_task_is_temp else f'{doris_catalog}_temp',
                'state': source_import_result['state'] if source_import_result else 'PENDING',
                'error_msg': error_message,
                'log_url': '',
                'group_file_size': group_file_size
            }
            BigqueryImportDorisTask.find_one_and_update(query, set_dict=update, upsert=True)


# 对某一轮的所有的gsc源文件进行分组
def divide_gsc_table_source_group(gsc_table_source_arr):
    group_file_size = 0
    table_source_group = []
    table_source_group_arr = []
    for index in range(0, len(gsc_table_source_arr)):
        gsc_table_source = gsc_table_source_arr[index]
        group_file_size = group_file_size + gsc_table_source['file_size']
        table_source_group.append(gsc_table_source)
        size_g = group_file_size / 1024
        if size_g > 20:
            table_source_group_arr.append(table_source_group)
            group_file_size = 0
            table_source_group = []
        # 全部分组已经完成，但是组合不超过80G
        if index == len(gsc_table_source_arr) - 1 and size_g <= 20:
            table_source_group_arr.append(table_source_group)

    return table_source_group_arr


def get_all_pending_export_data_task():
    query = {
        'state': 'PENDING'
    }
    export_data_tasks = list(BigqueryExportDataTask.find(query).limit(500))
    return export_data_tasks


def get_all_export_finish_data_task():
    query = {
        'state': 'EXPORT_FINISH'
    }
    loading_export_data_tasks = BigqueryExportDataTask.find_list(query=query)
    return loading_export_data_tasks


def get_all_loading_export_data_task():
    query = {
        'state': 'LOADING'
    }
    loading_export_data_tasks = BigqueryExportDataTask.find_list(query=query)
    return loading_export_data_tasks


def check_and_update_loading_export_data_task():
    loading_export_data_tasks = get_all_export_finish_data_task()
    for loading_export_data_task in loading_export_data_tasks:
        export_data_task_query = {
            '_id': loading_export_data_task['_id']
        }

        check_export_data_task = BigqueryExportDataTask.find_one(query=export_data_task_query)

        if check_export_data_task['state'] == 'LOADING':
            continue

        client = bigquery.Client(project='footprint-etl-internal')

        res = client._get_query_results(loading_export_data_task['job_id'], retry={})
        print(f'{loading_export_data_task["job_id"]} 任务检查状态结果为{res.complete}')

        if not res.complete:
            continue

        BigqueryExportDataTask.find_one_and_update(export_data_task_query, set_dict={'state': 'LOADING'})

        blobs = get_bigquery_gcs_object_list(loading_export_data_task)

        task_id = loading_export_data_task['_id']
        if len(list(blobs)) <= 0:
            print('检查loading任务结果:')
            print(f'{task_id} 任务产生的gcs结果为: 0')
            continue

        create_bigquery_gcs_source(loading_export_data_task)

        handle_create_doris_data_config(loading_export_data_task)

        BigqueryExportDataTask.find_one_and_update(export_data_task_query, set_dict={'state': 'FINISH'})


if __name__ == '__main__':
    auto_create_bigquery_to_doris_task()
