import os
import pymysql
from config import project_config
from utils.constant import PROJECT_PATH


def create_doris_temp_table(table_id: str):
    table_id = table_id.replace(':', '.')
    arr = table_id.split('.')
    project_name = arr[0]
    dataset_name = arr[1]
    table_name = arr[2]

    sql_abs_path = os.path.join(PROJECT_PATH, f'resources/stages/doris/sqls/{project_name}/{dataset_name}/{table_name}.sql')
    print(sql_abs_path)

    if not os.path.exists(sql_abs_path):
        print(f'{table_id} ***************** 不存在可以执行的sql文件，请检查')
        return

    content = get_create_doris_table_content(sql_abs_path)

    create_result = create_table(content)

    if create_result == 'success':
        print('您的临时表创建成功')

    print(create_result)


def get_create_doris_table_content(sql_abs_path):
    with open(sql_abs_path, 'r') as f:
        content = f.read()
        arr = content.split('not exists ')
        doris_table_name = arr[1]
        doris_table_name = doris_table_name[:doris_table_name.find('(')]
        content = content.replace(doris_table_name, f'{doris_table_name}_temp')
    print(content)
    return content


def create_table(sql):
    conn = pymysql.connect(host=project_config.doris_host,
                           port=project_config.doris_port,
                           user=project_config.doris_user,
                           password=project_config.doris_password,
                           database='example_db')
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    create_result = 'success'
    try:
        cursor.execute(sql)
        for r in cursor:
            print(r)
    except Exception as e:
        create_result = 'fail'
        print('create_table fail ******', sql, e)
    cursor.close()
    conn.close()
    return create_result
