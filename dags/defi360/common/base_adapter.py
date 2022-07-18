import datetime

import moment
from utils.constant import ENVIRONMENT
from utils.import_gsc_to_bigquery import (get_path_schema,
                                          import_gsc_to_bigquery_base)

from common.airflow_etl import AirFlowETL


# bq取数，合约取数（mock）+ upload csv + load 日更
class BaseAdapter:
    # prod
    project_id = 'project_id'
    data_set = 'data_set'
    # test
    project_id_test = 'project_id_test'
    data_set_test = 'data_set_test'

    bucket_name = 'bucket_name'
    # 时间分区字段
    time_partitioning_field = None
    # 项目名，用于本地daily数据存储的文件夹名
    project_name = None
    # 任务名，用于bigquery表名，daily数据文件名，airflow任务名
    task_name = None
    # 每日执行时间
    execution_time = None
    # 历史日期
    history_date = '2022-02-16'
    # bigquery表结构
    schema_name = ''
    # 历史取数时间范围，全量在子类置为空即可 history_day = ""
    history_day = 100
    # validate配置 按需取不同校验，前5个需要增加校验字段validate_field
    # 前5个需要增加校验字段（validate_field）
    # 最后一个连续性校验（连续性校验）不需要，置空即可
    # from defi360.validation.basic_validate.address_format_validate import AddressFormatValidate
    # from defi360.validation.basic_validate.field_not_null_validate import FieldNotNullValidate
    # from defi360.validation.basic_validate.abnormal_number_validate import AbnormalNumberValidate
    # from defi360.validation.basic_validate.negative_number_validate import NegativeNumberValidate
    # from defi360.validation.basic_validate.black_hole_address_validate import BlackHoleAddressValidate
    # from defi360.validation.basic_validate.data_continuity_validate import DataContinuityValidate
    # [{
    #         # 地址格式校验
    #         "cls": AddressFormatValidate,
    #         "validate_field": []
    #     }, {
    #         # 空值校验
    #         "cls": FieldNotNullValidate,
    #         "validate_field": []
    #     }, {
    #         # 异常值校验
    #         "cls": AbnormalNumberValidate,
    #         "validate_field": []
    #     }, {
    #         # 负值校验
    #         "cls": NegativeNumberValidate,
    #         "validate_field": []
    #     }, {
    #         # 黑洞地址校验  0x000...0000
    #         "cls": BlackHoleAddressValidate,
    #         "validate_field": []
    #     }, {
    #         # 连续性校验
    #         "cls": DataContinuityValidate,
    #         "validate_field": []
    #     }]
    validate_config = []

    # 初始化
    def __init__(self):
        self.etl = AirFlowETL(task_name=self.task_name, project_name=self.task_name)

    def get_bigquery_table_name(self):
        production_name = "{}.{}.{}".format(self.project_id, self.data_set, self.task_name)
        test_name = "{}.{}.{}".format(self.project_id_test, self.data_set_test, self.task_name)
        return production_name if (ENVIRONMENT == 'production' or ENVIRONMENT == 'prod') else test_name

    def get_bigquery_daily_table_name(self):
        return "{table_name}_daily".format(table_name=self.get_bigquery_table_name())

    def get_bigquery_history_table_name(self):
        return "{table_name}_history".format(table_name=self.get_bigquery_table_name())

    # 导入csv到gsc
    def do_upload_csv_to_gsc(self, date_str: str = None):
        self.etl.do_upload_csv_to_gsc(bucket_name=self.bucket_name, date_str=date_str)
        print('upload daily csv to gsc done.')

    # 从gsc导入csv到bigquery
    def do_import_gsc_to_bigquery(self):
        uri = "gs://{bucket_name}/{gsc_folder}/*.csv".format(
            bucket_name=self.bucket_name,
            gsc_folder=self.task_name
        )
        import_gsc_to_bigquery_base(
            schema=get_path_schema(self.schema_name),
            uri=uri,
            bigquery_table_name=self.get_bigquery_daily_table_name(),
            time_partitioning_field=self.time_partitioning_field
        )
        print('import gsc csv to bigquery done.')

    def get_replace_sql_date(self):
        replace_date = "< '{date}'"
        if self.history_day:
            date = datetime.datetime.strptime(self.history_date, '%Y-%m-%d')
            start_date = moment.utc(date).add('days', -self.history_day).format('YYYY-MM-DD')
            end_date = moment.utc(date).add('days', -1).format('YYYY-MM-DD')
            replace_date = " BETWEEN DATE('{start_date}') AND DATE('{end_date}')".format(
                end_date=end_date,
                start_date=start_date
            )

        run_date = replace_date.format(date=self.history_date)
        return run_date

    def load_daily_data(self, run_date: str = None):
        pass

    # 校验
    def validate(self):
        for config in self.validate_config:
            config["cls"]({
                "validate_field": config["validate_field"],
                "validate_table": self.get_bigquery_table_name(),
                "project": self.task_name
            }).validate()

    def airflow_steps(self):
        return [
            self.load_daily_data
        ]

    def airflow_dag_params(self):
        dag_id = "DeFi360_{task_name}_dag".format(task_name=self.task_name)
        if self.project_id == "gaia-data":
            dag_id = "DeFi360_{project_id}_{data_set}_{task_name}_dag".format(
                project_id=self.project_id,
                data_set=self.data_set,
                task_name=self.task_name
            )
        dag_params = {
            "dag_id": dag_id,
            "catchup": False,
            "schedule_interval": self.execution_time,
            "description": "DeFi360_{task_name}_dag".format(task_name=self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': datetime.timedelta(minutes=10),
                'start_date': datetime.datetime(2022, 1, 1)
            },
            "dagrun_timeout": datetime.timedelta(days=30),
            'tags': ['DeFi360']
        }
        print('dag_params', dag_params)
        return dag_params
