from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.query_bigquery import query_bigquery
from utils.date_util import DateUtil
from utils.monitor import send_to_slack
from utils import Constant
import pandas
from config import project_config
from models import MonitorDashBoard
from datetime import timedelta, datetime
from models import BigQueryCheckPoint
from utils.gql.gql_monitor_task_execution import GQLMonitorTaskExecution
from utils.gql.gql_task_execution_flag import GQLTaskExecutionFlag

"""
1、任务开始
2、取数
3、数据检查
    a.通用检查
    b.自定义校验
4、create csv
5、upload csv
6、import bigquery
7、任务结束
"""


class ETLBasic(object):
    valid_less_than_zero_fields = []
    valid_null_fields = []
    table_name = ''
    task_name = ''
    task_airflow_execution_time = ''
    columns = []
    remark = ''
    task_category = ''

    def __init__(self, execution_date: datetime = None):
        if not execution_date:
            execution_date = self.get_execution_date()
        self.execution_date = execution_date
        self.execution_date_str = self.execution_date.strftime('%Y-%m-%d')

    def get_execution_date(self):
        return DateUtil.utc_start_of_date()

    def airflow_dag_params(self, task_airflow_execution_time: str = None):
        dag_params = {
            "dag_id": "footprint_{}_dag".format(self.task_name),
            "catchup": False,
            "schedule_interval": task_airflow_execution_time or self.task_airflow_execution_time,
            "description": "{}_dag".format(self.task_name),
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'on_failure_callback': self.on_failure_callback,
                'retry_delay': timedelta(minutes=5),
                'start_date': datetime(2021, 7, 1)
            },
            "dagrun_timeout": timedelta(days=30)
        }
        return dag_params

    def exec_local(self):
        self.do_start()
        
        self.do_scrapy_data()
        
        self.do_upload_csv_to_gsc()
        
        self.do_import_gsc_to_bigquery()
        
        self.do_common_valid()
        
        self.do_customize_valid()
        
        self.do_alarm()
        
        self.do_end()

        self.do_task_execution_flag()

    def airflow_steps(self):
        return [
            self.do_start,
            self.do_scrapy_data,
            self.do_upload_csv_to_gsc,
            self.do_import_gsc_to_bigquery,
            self.do_common_valid,
            self.do_customize_valid,
            self.do_alarm,
            self.do_end,
            self.do_task_execution_flag
        ]

    def on_failure_message(self, context):
        return ''

    def on_failure_callback(self, context):
        message = self.on_failure_message(context)
        if not message:
            return
        send_to_slack(message)

    """任务开始"""

    def do_start(self):
        """任务开始的操作"""
        rule_name = Constant.DASH_BOARD_RULE_NAME['TASK_EXECUTION']
        desc = '{task_name} rule_name is task execution'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['TASK_EXECUTION']
        item_value = 1
        result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION']
        sql = ''
        self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql)


    def do_alarm(self):
        pass

    """通用数据校验"""
    """required_keys schema required 类型 """
    """gap_keys 不能存在暴涨暴跌的数据"""

    def do_common_valid(self):
        self.do_core_field_not_valid()
        self.do_core_field_anomaly_valid()
        self.do_data_continuity_valid()

    """核心字段不能有null值"""

    def do_core_field_not_valid(self):
        query_str = self.data_is_null_sql()
        if not query_str:
            return

        df_from_bigquery = query_bigquery(query_str)
        is_exists_null_core_field = any(df_from_bigquery.question_type == "null_value")
        """找出所有的null记录"""
        core_field_null_result = df_from_bigquery.loc[df_from_bigquery['question_type'] == 'null_value']

        is_exists_less_than_zero_core_field = any(df_from_bigquery.question_type == "less_than_zero")
        """找出所有的记录"""
        core_field_less_zero_result = df_from_bigquery.loc[df_from_bigquery['question_type'] == 'less_than_zero']

        """保存监控记录结果"""
        self.save_data_field_monitor(core_field_null_result, core_field_less_zero_result)

        if is_exists_null_core_field:
            self.handle_core_field_exists_null_check(core_field_null_result)

        if is_exists_less_than_zero_core_field:
            self.handle_core_field_exists_less_than_zero_check(core_field_less_zero_result)

    def handle_core_field_exists_null_check(self, core_field_null_result):
        pass

    def handle_core_field_exists_less_than_zero_check(self, core_field_less_zero_result):
        pass

    """核心字段与前一天相比不能有差距过大的值"""

    def do_core_field_anomaly_valid(self):
        query_str = self.anomaly_sql()
        if not query_str:
            return
        df_from_bigquery = query_bigquery(query_str)

        self.save_data_anomaly_monitor(anomaly_result=df_from_bigquery)

        if not df_from_bigquery.empty:
            self.handle_exists_anomaly_check(anomaly_result=df_from_bigquery)

    def handle_exists_anomaly_check(self, anomaly_result):
        pass

    """数据连续检查"""

    def do_data_continuity_valid(self):
        query_str = self.continuity_sql()
        if not query_str:
            return
        df_from_bigquery = query_bigquery(query_str)

        is_duplicate_data = any(df_from_bigquery.question_type == "duplication_data")
        duplicate_data_result = df_from_bigquery.loc[df_from_bigquery['question_type'] == 'duplication_data']

        is_missing_data = any(df_from_bigquery.question_type == "missing_data")
        missing_data_result = df_from_bigquery.loc[df_from_bigquery['question_type'] == 'missing_data']

        self.save_data_continuity_monitor(duplicate_data_result, missing_data_result)

        if is_duplicate_data:
            self.handle_exists_duplicate_check(duplicate_data_result)

        if is_missing_data:
            self.handle_exists_missing_data_check(missing_data_result)

    def handle_exists_missing_data_check(self, missing_data_result):
        pass

    def handle_exists_duplicate_check(self, duplicate_data_result):
        pass

    """自定义数据校验"""

    def do_customize_valid(self, df=None):
        pass

    """数据保存到csv"""

    def do_write_csv(self, values=[]):
        df = pandas.DataFrame(values, columns=self.columns)
        csv_file = self.get_csv_file_name()
        print('do_write_csv', csv_file)
        print(df)
        df.to_csv(csv_file, index=False, header=True)

    """上传csv文件"""

    def do_upload_csv_to_gsc(self):
        source_csv_file = self.get_csv_file_name()
        destination_file_path = '{name}/date={execution_date}/{name}_{execution_date}.csv'.format(name=self.task_name,
                                                                                                  execution_date=self.execution_date_str)
        upload_csv_to_gsc(source_csv_file, destination_file_path)

    def upload_csv_to_gsc_with_cache(self, source_csv_file, destination_file_path):
        if BigQueryCheckPoint.has_check_point(destination_file_path, self.execution_date_str):
            return False
        upload_csv_to_gsc(source_csv_file, destination_file_path)
        BigQueryCheckPoint.set_check_point(destination_file_path, self.execution_date_str)

    def get_csv_file_name(self):
        return ''

    """数据导入到bigquery"""

    def do_import_gsc_to_bigquery(self):
        import_gsc_to_bigquery(name=self.task_name)

    """任务结束"""

    def do_end(self):
        """任务结束的操作"""
        rule_name = Constant.DASH_BOARD_RULE_NAME['TASK_EXECUTION']
        desc = '{task_name} rule_name is task execution'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['TASK_EXECUTION']
        item_value = 0
        result_code = Constant.DASH_BOARD_RESULT_CODE['REGULAR']
        sql = ''
        self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql)

    def do_task_execution_flag(self):
        if not self.task_category:
            return
        params = {
            'task_name': self.task_name,
            'association_table': self.table_name,
            'category': self.task_category,
            'data_last_time': self.execution_date,
            'last_updated_by': 'system',
            'last_updated_at': DateUtil.utc_current()
        }
        GQLTaskExecutionFlag().insertTaskExecutionFlag(params)

    def data_is_null_sql(self):
        return ''

    def continuity_sql(self):
        return ''

    def anomaly_sql(self):
        return ''

    def save_data_field_monitor(self, core_field_null_result, core_field_less_zero_result):
        self.save_data_null_valid_monitor_result(core_field_null_result)
        self.save_data_less_than_zero_monitor_result(core_field_less_zero_result)

    def save_data_null_valid_monitor_result(self, core_field_null_result):
        rule_name = Constant.DASH_BOARD_RULE_NAME['FIELD_LEGAL_NULL']
        desc = '{task_name} rule_name is legal field null'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['FIELD_LEGAL_NULL']
        sql = self.data_is_null_sql()

        for valid_null_field in self.valid_null_fields:
            item_value = int(core_field_null_result[valid_null_field].isnull().sum())
            result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION'] if item_value > 0 else Constant.DASH_BOARD_RESULT_CODE['REGULAR']
            self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql, valid_null_field)

    def save_data_less_than_zero_monitor_result(self, core_field_less_zero_result):
        rule_name = Constant.DASH_BOARD_RULE_NAME['FIELD_LEGAL_LESS_THAN_ZERO']
        desc = '{task_name} rule_name is legal field less than zero'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['FIELD_LEGAL_LESS_THAN_ZERO']
        sql = self.data_is_null_sql()

        for valid_less_than_zero_field in self.valid_less_than_zero_fields:
            item_value = int((core_field_less_zero_result[valid_less_than_zero_field] < 0).sum())
            result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION'] if item_value > 0 else Constant.DASH_BOARD_RESULT_CODE['REGULAR']
            self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql, valid_less_than_zero_field)

    def save_data_continuity_monitor(self, duplicate_data_result, missing_data_result):
        self.save_data_duplicate_monitor_result(duplicate_data_result)
        self.save_data_missing_monitor_result(missing_data_result)

    def save_data_duplicate_monitor_result(self, duplicate_data_result):
        rule_name = Constant.DASH_BOARD_RULE_NAME['FIELD_CONTINUITY']
        item_value = len(duplicate_data_result)
        result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION'] if item_value > 0 else Constant.DASH_BOARD_RESULT_CODE['REGULAR']
        desc = '{task_name} rule_name data duplicate valid'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['FIELD_CONTINUITY'] + '_重复'
        sql = self.continuity_sql()
        self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql)

    def save_data_missing_monitor_result(self, missing_data_result):
        rule_name = Constant.DASH_BOARD_RULE_NAME['FIELD_CONTINUITY']
        item_value = len(missing_data_result)
        result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION'] if item_value > 0 else Constant.DASH_BOARD_RESULT_CODE['REGULAR']
        desc = '{task_name} rule_name data missing valid'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['FIELD_CONTINUITY'] + '_缺失'
        sql = self.continuity_sql()
        self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql)

    def save_data_anomaly_monitor(self, anomaly_result):
        rule_name = Constant.DASH_BOARD_RULE_NAME['FIELD_ANOMALY']
        item_value = len(anomaly_result)
        result_code = Constant.DASH_BOARD_RESULT_CODE['EXCEPTION'] if item_value > 0 else Constant.DASH_BOARD_RESULT_CODE['REGULAR']
        desc = '{task_name} rule_name data anomaly valid'.format(task_name=self.task_name)
        desc_cn = Constant.DASH_BOARD_RULE_NAME_DESC_CN['FIELD_ANOMALY']
        sql = self.anomaly_sql()
        self.save_monitor(rule_name, item_value, result_code, desc, desc_cn, sql)

    def save_monitor(self, rule_name: str, item_value: int, result_code: int, desc: str, desc_cn: str, sql: str, field: str = None):
        query = {
            'task_name': self.task_name,
            'rule_name': rule_name,
            'field': field or '',
            'stats_date': DateUtil.utc_start_of_date(self.execution_date)
        }

        update = {
            'database_name': project_config.bigquery_etl_database,
            'table_name': self.table_name,
            'desc': desc,
            'item_value': item_value,
            'result_code': result_code,
            'sql': sql,
            'desc_cn': desc_cn + self.remark
        }
        MonitorDashBoard.update_one(query=query, set_dict=update, upsert=True)

        z = query.copy()
        z.update(update)

        gql_result = GQLMonitorTaskExecution().insertMonitorTaskExecution(z)

        print(gql_result)
