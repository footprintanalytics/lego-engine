import pandas
import os
from utils.constant import PROJECT_PATH
from models import TableInfo


# 读取csv配置信息
def batch_import_table_info():
    table_info_path = 'doris_load/table_info.csv'
    sql_path = os.path.join(PROJECT_PATH, table_info_path)
    df = pandas.read_csv(sql_path, header=0, sep=',')

    for index, row in df.iterrows():
        table_path = row['table_path']
        select_key = row['select_key']
        gcs_root_path = row['gcs_root_path']
        gcs_bucket_name = row['gcs_bucket_name']
        project = row['project']
        dataset = row['dataset']
        table_name = row['table_name']
        _type = row['type']
        export_type = row['export_type']
        async_min_time = row['async_min_time']

        query = {
            'table_path': table_path,
            'gcs_bucket_name': gcs_bucket_name
        }

        update = {
            'select_key': select_key if not pandas.isna(select_key) else '',
            'gcs_root_path': gcs_root_path,
            'project': project,
            'dataset': dataset,
            'table_name': table_name,
            'type': _type,
            'export_type': export_type,
            'async_min_time': async_min_time
        }

        print(update)

        TableInfo.find_one_and_update(query=query, set_dict=update, upsert=True)


if __name__ == '__main__':
    batch_import_table_info()
