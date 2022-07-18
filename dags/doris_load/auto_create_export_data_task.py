from models import TableInfo, BigqueryExportDataTask, BigqueryLastModifyRecord
from utils.date_util import DateUtil
from datetime import datetime
from google.cloud import bigquery
import pytz
import pydash
from utils.query_bigquery import query_bigquery
from doris_load.check_bigquery_doris_row_count import check_bigquery_doris_row_count


def auto_create_export_data_task():
    all_every_day_table_infos = get_all_every_day_table_info()

    for every_day_table_info in all_every_day_table_infos:
        try:
            check = check_and_save_last_modify_record(every_day_table_info)
            if check:
                print(every_day_table_info)
                handle_create_export_data_task(every_day_table_info)
            else:
                check_bigquery_doris_row_count(every_day_table_info['table_path'], check_date=DateUtil.utc_current())
        except Exception as e:
            print('auto_create_export_data_task fail ************')
            print(e)


def check_everyday_once_time_rule(export_type, every_day_table_info):
    if export_type != 'everyday_once':
        return True

    table_path = every_day_table_info['table_path']

    async_min_time = every_day_table_info['async_min_time']

    today_start_date = DateUtil.utc_start_of_date()

    today_end_date = DateUtil.utc_end_of_date()

    current_date = DateUtil.utc_current()
    today_start_date_str = DateUtil.utc_start_of_date().strftime('%Y-%m-%d')

    async_min_date = datetime.strptime(f'{today_start_date_str} {async_min_time}:00.00+00:00',
                                       '%Y-%m-%d %H:%M:%S.%f%z')

    async_max_date = DateUtil.utc_x_minutes_after(5, async_min_date)

    query = {
        'table_path': table_path,
        'createdAt': {'$gte': today_start_date, '$lte': today_end_date}
    }

    export_data_task = BigqueryExportDataTask.find_one(query=query)

    if export_data_task:
        return False

    return current_date > async_max_date


def check_everyday_twice_time_rule(export_type, table_name):
    if export_type != 'everyday_twice':
        return True

    current_date = DateUtil.utc_current()
    today_start_date = DateUtil.utc_start_of_date().strftime('%Y-%m-%d')

    time_frames = [
        {
            'start': '13:00',
            'end': '13:30'
        },
        {
            'start': '23:00',
            'end': '23:30'
        }
    ]

    time_frame_check = False

    for time_frame in time_frames:
        start = time_frame['start']
        end = time_frame['end']
        async_min_date = datetime.strptime(f'{today_start_date} {start}:00.00+00:00',
                                           '%Y-%m-%d %H:%M:%S.%f%z')
        async_max_date = datetime.strptime(f'{today_start_date} {end}:00.00+00:00',
                                           '%Y-%m-%d %H:%M:%S.%f%z')

        if async_min_date < current_date < async_max_date:
            frame_query = {
                'table_path': table_name,
                'createdAt': {'$gte': async_min_date, '$lte': async_max_date}
            }
            frame_export_data_task = BigqueryExportDataTask.find_one(query=frame_query)

            time_frame_check = False if frame_export_data_task else True
            break
    return time_frame_check


def handle_create_export_data_task(every_day_table_info):
    table_path = every_day_table_info['table_path']
    async_min_time = every_day_table_info['async_min_time']
    dataset = every_day_table_info['dataset']
    export_type = every_day_table_info['export_type']
    gcs_root_path = every_day_table_info['gcs_root_path']
    gcs_bucket_name = every_day_table_info['gcs_bucket_name']
    project = every_day_table_info['project']
    select_key = every_day_table_info['select_key']
    table_name = every_day_table_info['table_name']
    _type = every_day_table_info['type']
    strategy = pydash.get(every_day_table_info, 'strategy', 'upsert')
    async_days = pydash.get(every_day_table_info, 'async_days', 1)
    is_temp = strategy == 'delete_insert'

    # 如果导出策略设置为all 的，每天都需要导出全量数据
    export_data_start_date = datetime.strptime('1990-01-01', '%Y-%m-%d') if (not select_key or export_type == 'all') else DateUtil.utc_start_of_date(DateUtil.utc_x_hours_ago(async_days * 24))
    export_data_end_date = datetime.strptime('2900-01-01', '%Y-%m-%d') if (not select_key or export_type == 'all') else DateUtil.utc_start_of_date()

    # export_data_start_date = datetime.strptime('1905-01-01', '%Y-%m-%d')
    # export_data_end_date = datetime.strptime('2900-01-01', '%Y-%m-%d')

    query = {
        'table_path': table_path,
        'gcs_bucket_name': gcs_bucket_name,
        'export_data_start_date': export_data_start_date,
        'export_data_end_date': export_data_end_date,
        'state': 'PENDING'
    }

    # 如果存在同条件pending的数据，不生成任务
    query_export_data_task = BigqueryExportDataTask.find_one(query=query)
    if query_export_data_task:
        return

    item = {
        'table_path': table_path,
        'gcs_bucket_name': gcs_bucket_name,
        'export_data_start_date': export_data_start_date,
        'export_data_end_date': export_data_end_date,
        'async_min_time': async_min_time,
        'state': 'PENDING',
        'gcs_root_path': gcs_root_path,
        'strategy': strategy,
        'is_temp': is_temp
    }
    BigqueryExportDataTask.insert_one(item)


def get_all_every_day_table_info():
    query = {
        'status': {'$ne': 'disable'},
        'export_type': {'$in': ['everyday', 'all', 'everyday_twice', 'everyday_once']}
    }
    every_day_table_infos = TableInfo.find_list(query=query)
    return every_day_table_infos


def check_and_save_last_modify_record(table_info):
    table_name = table_info['table_path']
    _type = pydash.get(table_info, 'type', 'TABLE')
    table_name = table_name.replace(':', '.')
    export_type = table_info['export_type']

    # footprint_last_updated_at 每次都同步
    if table_name == 'gaia-data.gaia.footprint_last_updated_at':
        print(f'特殊表结构，触发同步 ************* {table_name}')
        return True

    check_once_result = check_everyday_once_time_rule(export_type, table_info)

    if not check_once_result:
        return False

    check_twice_result = check_everyday_twice_time_rule(export_type, table_name)

    if not check_twice_result:
        return False

    client = bigquery.Client(project='footprint-etl-internal')
    result = client.get_table(table_name)
    modify_time = result.modified
    row_count = result.num_rows

    query = {
        'table_name': table_name
    }

    if _type == 'VIEW':
        sql = f'select count(*) as count from {table_name}'
        sql_result = query_bigquery(sql)
        sql_result = sql_result.to_dict('records')
        row_count = sql_result[0]['count']

    last_modify_record_list = list(BigqueryLastModifyRecord.find(query).sort([('last_modify_time', -1), ('_id', -1)]).limit(1))

    if len(last_modify_record_list) <= 0:
        save_last_modify_record(table_name, _type, modify_time, row_count)
        return True

    last_modify_record = last_modify_record_list[0]

    if _type == 'TABLE' and last_modify_record['last_modify_time'].replace(tzinfo=pytz.timezone('UTC')) < modify_time.replace(tzinfo=pytz.timezone('UTC')):
        save_last_modify_record(table_name, _type, modify_time, row_count)
        return True
    print('获取bigquery 上最新的数据 row_count', row_count)
    print('获取系统记录的最新数据 row_count', pydash.get(last_modify_record, 'row_count', 0))
    if _type == 'VIEW' and row_count != pydash.get(last_modify_record, 'row_count', 0):
        save_last_modify_record(table_name, _type, modify_time, row_count)
        return True
    return False


# 如果是view to table 类型，就保存row_count
def save_last_modify_record(table_name, _type, modify_time, row_count):
    query = {
        'table_name': table_name,
        'last_modify_time': modify_time,
        'type': _type
    }
    update = {
        'row_count': row_count
    }

    BigqueryLastModifyRecord.find_one_and_update(query=query, set_dict=update, upsert=True)


if __name__ == '__main__':
    auto_create_export_data_task()
