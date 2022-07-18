import os

from basic.etl_basic import ETLBasic
from utils.date_util import DateUtil
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from utils.upload_csv_to_gsc import upload_to_gsc
from utils.query_bigquery import query_bigquery
from utils.constant import PROJECT_PATH
from pathlib import Path
from datetime import datetime, timedelta
from joblib import Parallel, delayed
import multiprocessing
import tqdm


class GamefiProtocolDailyStats(ETLBasic):
    task_airflow_execution_time = '50 3 * * *'
    task_name = 'gamefi_protocol_daily_stats'
    table_name = 'gamefi_protocol_daily_stats'
    task_category = 'gamefi'
    data = []

    def exec(self):
        self.format_data_to_csv()
        self.handle_upload_csv_to_gsc()
        self.do_import_gsc_to_bigquery()

    def get_execution_date(self):
        return DateUtil.utc_start_of_date(DateUtil.utc_24h_ago())

    def do_scrapy_data(self):
        sql = f"""
        WITH new_user AS (
  SELECT 
    protocol_name, 
    protocol_slug, 
    chain, 
    DATE(min_day) AS day, 
    COUNT(DISTINCT wallet_address) AS new_users 
  FROM 
    (
      SELECT 
        protocol_name, 
        protocol_slug, 
        chain, 
        wallet_address, 
        MIN(block_timestamp) AS min_day, 
      FROM 
        `xed-project-237404.footprint.gamefi_protocol_transaction` 
      GROUP BY 1, 2, 3, 4
    ) 
  GROUP BY 1, 2, 3, 4
), 
unique_user AS (
  SELECT 
    protocol_name, 
    protocol_slug, 
    chain, 
    DATE(block_timestamp) AS day, 
    COUNT(DISTINCT wallet_address) AS unique_users, 
  FROM 
    `xed-project-237404.footprint.gamefi_protocol_transaction` 
  GROUP BY 1, 2, 3, 4
)
select
  *,
  SUM(new_users) OVER(PARTITION BY protocol_slug, chain  ORDER BY day) AS total_users
from 
  (
    SELECT 
      unique_user.*, 
      coalesce(new_user.new_users, 0) AS new_users, 
    FROM 
      unique_user 
      LEFT JOIN new_user ON unique_user.day = new_user.day 
      AND unique_user.protocol_slug = new_user.protocol_slug 
      AND unique_user.chain = new_user.chain
  )
        """
        self.data = query_bigquery(sql)
        print('完成bigquery取数')

    def format_data_source(self):
        pass

    def fix_scrapy_data(self):
        """可以根据实际业务自行做数据的结构调整"""
        pass

    def format_data_to_csv(self):
        print(f"data to csv ,date = {self.execution_date_str},len = {len(self.data)}")
        csv_file = self.get_csv_name()
        self.data.to_csv(csv_file, index=False, header=True)

    def get_csv_name(self):
        csv_dir_path = os.path.join(PROJECT_PATH, '../data/{}'.format(self.task_name))
        Path(csv_dir_path).mkdir(parents=True, exist_ok=True, mode=0o777)
        return os.path.join(csv_dir_path, './all.csv')

    def handle_upload_csv_to_gsc(self):
        file_name = self.get_csv_name()
        destination_file_path = f'{self.table_name}/all.csv'
        print(f"上传 csv: {file_name} -> gsc:{destination_file_path}")
        upload_to_gsc(file_name, destination_file_path)

    def on_failure_message(self, context):
        return f'Gamefi Protocol Transaction 任务执行失败，影响范围: 暂无增量数据，需关注的关键维度: {self.task_name}'

    def airflow_steps(self):
        return [
            self.exec,
            self.do_task_execution_flag
        ]


if __name__ == '__main__':
    for fn in GamefiProtocolDailyStats().airflow_steps():
        fn()
    print('run success')
