'''
@Project ：defi-up 
@File    ：pool_daily_stats_view_builder.py
@Author  ：Nick
@Date    ：2021/8/24 3:11 下午 
'''
import os
from datetime import timedelta

import moment

from utils.constant import PROJECT_PATH
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from utils.upload_csv_to_gsc import upload_csv_to_gsc


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            print('已存在目录,跳过')


class AirFlowETL:
    task_name: str
    project_name: str
    date_str: str

    def __init__(self, task_name: str, project_name: str, date_str: str = None):
        # 默认是昨天
        if date_str is None:
            self.date_str = self.get_execution_date()
        else:
            self.date_str = date_str
        self.task_name = task_name
        self.project_name = project_name

    def get_execution_date(self):
        now = moment.utcnow().datetime
        yesterday = now - timedelta(days=1)  # 获得前一天的时间
        execution_date = yesterday.strftime("%Y-%m-%d")
        # print('now', now)
        # print('yesterday', yesterday)
        # print('execution_date', execution_date)
        return execution_date

    def get_csv_file_name(self, date_str=None):
        dags_folder = PROJECT_PATH
        if not date_str:
            date_str = self.date_str

        return os.path.join(dags_folder, '../data/{project_name}/{task_name}-{date}.csv'.format(
            project_name=self.project_name.lower(),
            task_name=self.task_name.lower(),
            date=date_str
        ))

    def is_data_file_exists(self,  date_str: str = None):
        return os.path.isfile(self.get_csv_file_name(date_str))

    def save_data(self, df, date_str: str = None):
        ensure_dir(self.get_csv_file_name(date_str))
        print('save csv file to ', self.get_csv_file_name(date_str))
        df.to_csv(self.get_csv_file_name(date_str), index=False)

    def do_upload_csv_to_gsc(self, bucket_name: str = None, date_str: str = None):
        print('do_upload_csv_to_gsc')
        source_csv_file = self.get_csv_file_name(date_str=date_str)
        if not date_str:
            date_str = self.date_str
        destination_file_path = '{name}/{name}_{execution_date}.csv'.format(
            name=self.task_name,
            execution_date=date_str)
        upload_csv_to_gsc(source_csv_file, destination_file_path, bucket_name)

    def do_import_gsc_to_bigquery(
        self,
        schema_name: str = 'pool_transactions',
        project_id: str = None,
        project_name: str = None,
        bucket_name: str = None
    ):
        print('do_import_gsc_to_bigquery ', self.task_name)
        import_gsc_to_bigquery(
            name=self.task_name,
            schema_name=schema_name,
            custom_data_base=project_name,
            project_id=project_id,
            bucket_name=bucket_name
        )
