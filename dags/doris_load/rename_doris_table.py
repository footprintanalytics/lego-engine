from models import BigqueryExportDataTask, BigqueryImportDorisTask
import pymysql
from config import project_config
import pydash


def rename(export_data_task_id: str, table_name: str):
    check = check_rename_export_data_task(export_data_task_id, table_name)
    if not check:
        return

    rename_doris_table(table_name)


def doris_query_rows(sql):
    conn = pymysql.connect(host=project_config.doris_host,
                           port=project_config.doris_port,
                           user=project_config.doris_user,
                           password=project_config.doris_password,
                           database='example_db')
    result = None
    cursor = conn.cursor()
    rows = cursor.execute(sql)
    for r in cursor:
        print(r)
        result = r[0]
    cursor.close()
    conn.close()

    return result


def rename_doris_table(table_name: str):
    arr = table_name.replace(':', '.').split('.')
    project = arr[0]
    dataset = arr[1]
    table = arr[2]
    database = f"{project.replace('-', '_')}__{dataset.replace('-', '_')}"

    is_exists_temp_sql = f'select count(*) as count from information_schema.TABLES where TABLE_SCHEMA = "{database}" and TABLE_NAME = "{table}_temp"'

    result = doris_query_rows(is_exists_temp_sql)

    if result == 0:
        print(f'{table_name} 找不到temp表不允许执行删除!!!!')
        return

    drop_table_sql = f"""
            drop table {database}.{table}
        """
    print('drop_table_sql', drop_table_sql)

    try:
        doris_query_rows(drop_table_sql)
    except Exception as e:
        print(f"table {table} drop already dorp")

    rename_table = f"""
        alter table {database}.{table}_temp rename {table}
        """
    print('rename_table', rename_table)
    doris_query_rows(rename_table)


# 检查需要执行rename的任务信息
def check_rename_export_data_task(export_data_task_id: str, table_name: str, is_skip_strategy_check=False):
    export_data_task = BigqueryExportDataTask.find_by_id(export_data_task_id)
    if not export_data_task:
        return False

    table_path = export_data_task['table_path']
    if table_path != table_name:
        print(f'rename {export_data_task_id} 失败，目标删除表{table_name}与export_data_task的表不一致...')
        return False

    export_data_task_state = export_data_task['state']

    if export_data_task_state != 'FINISH':
        print(f'{export_data_task_id} 任务状态没有完成！！')
        return False

    if not is_skip_strategy_check:
        strategy = pydash.get(export_data_task, 'strategy', 'upsert')

        if strategy != 'delete_insert':
            return False

        is_temp = pydash.get(export_data_task, 'is_temp', False)

        if not is_temp:
            print(f'{export_data_task_id}临时表数据不存在!!!，不能执行修改表名')
            return True

    import_data_tasks = BigqueryImportDorisTask.find_list({'export_data_task_id': export_data_task_id})

    if len(import_data_tasks) <= 0:
        print(f'{export_data_task_id} 没有产生导出数据的任务！！')
        return False

    all_finish = True
    for import_data_task in import_data_tasks:
        import_data_task_state = import_data_task['state']
        if import_data_task_state != 'FINISHED':
            all_finish = False
            break

    return all_finish


if __name__ == '__main__':
    rename('629754009645bc40c38ad119', 'gaia-data:gaia.izumi_pool_holders')
