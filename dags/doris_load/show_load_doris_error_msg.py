from models import BigqueryImportDorisTask
import pymysql
from config import project_config
import pandas

def show_load_doris_error_msg():
    bigquery_import_doris_tasks = get_cancel_bigquery_import_doris_task()

    all_result = []
    for bigquery_import_doris_task in bigquery_import_doris_tasks:
        print(bigquery_import_doris_task)
        doris_catalog = bigquery_import_doris_task['doris_catalog']
        label = bigquery_import_doris_task['label']
        database = doris_catalog.split('.')[0]
        cancel_log = get_cancel_log(database, label)
        print(cancel_log)
        all_result.append(cancel_log)
    df = pandas.DataFrame(all_result, columns=['label', 'state', 'error_msg', 'create_time', 'etl_start_time', 'etl_finish_time', 'load_start_time', 'load_end_time', 'log_url'])
    csv_file = './show_load_doris_error_msg.csv'
    df.to_csv(csv_file, index=False, header=True)





def get_cancel_log(database, label):
    _mysql_conn = mysql_conn(database)
    sql = f'show load where Label="{label}" order by CreateTime Desc;'
    cursor = _mysql_conn.cursor()
    cursor.execute(sql)
    for r in cursor:
        # label 状态第三个字段
        label = r[1]
        state = r[2]
        error_msg = r[7]
        create_time = r[8]
        etl_start_time = r[9]
        etl_finish_time = r[10]
        load_start_time = r[11]
        load_end_time = r[12]
        log_url = r[13]
        print(label, state, error_msg, create_time, etl_start_time, etl_finish_time, load_start_time, load_end_time)
    cursor.close()
    return { 'log_url': log_url, 'label': label, 'state': state, 'error_msg': error_msg, 'create_time': create_time, 'etl_start_time': etl_start_time, 'etl_finish_time': etl_finish_time, 'load_start_time': load_start_time, 'load_end_time': load_end_time }


def mysql_conn(database):
    # 打开数据库连接
    conn = pymysql.connect(host=project_config.doris_host,
                           port=project_config.doris_port,
                           user=project_config.doris_user,
                           password=project_config.doris_password,
                           database=database)
    return conn


def get_cancel_bigquery_import_doris_task():
    query = {
        'state': 'CANCELLED'
    }
    bigquery_import_doris_task = BigqueryImportDorisTask.find_list(query=query, projection={'label': 1, 'doris_catalog': 1, 'createdAt': 1, 'updatedAt': 1})
    return bigquery_import_doris_task


if __name__ == '__main__':
    show_load_doris_error_msg()
