import requests

from utils.date_util import DateUtil
from models import MonitorDashBoard
from utils.gql.gql_monitor_task_execution import GQLMonitorTaskExecution

members = {
    'Herwin': 'U010HS22HNV',
    'Leo': 'U010VF8JF9C',
    'Jakin': 'U010W22C1JQ',
    'Aison': 'U010W410HR6'
}


def send_to_slack(text):
    url = 'https://'
    requests.post(url=url, json={'text': text})


def send_to_slack_and_remind_all(text):
    send_to_slack(text + '\n <!channel>')


def save_monitor(task_name, execution_date, bigquery_etl_database, table_name, rule_name: str, item_value: int,
                 result_code: int, desc: str,
                 desc_cn: str,
                 sql: str,
                 field: str = None):
    query = {
        'task_name': task_name,
        'rule_name': rule_name,
        'field': field or '',
        'stats_date': DateUtil.utc_start_of_date(execution_date)
    }

    update = {
        'database_name': bigquery_etl_database,
        'table_name': table_name,
        'desc': desc,
        'item_value': item_value,
        'result_code': result_code,
        'sql': sql,
        'desc_cn': desc_cn
    }
    MonitorDashBoard.update_one(query=query, set_dict=update, upsert=True)

    z = query.copy()
    z.update(update)

    gql_result = GQLMonitorTaskExecution().insertMonitorTaskExecution(z)

    print(gql_result)
