from datetime import timedelta, datetime
from utils.build_dag_util import BuildDAG
from sync.rescan_fieldvalues.rescan_fieldvalues import RescanFieldvalues
from utils.monitor import send_to_slack


def python_callable():
    RescanFieldvalues().exec()


def on_failure_callback(context):
    message = f'footprint_rescan_fieldvalues 任务执行失败，影响范围:  metabase 使用的搜索中fieldvalues 不会自动更新'
    send_to_slack(message)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 7, 1),
    'on_failure_callback': on_failure_callback
}

dag_params = {
    "dag_id": "footprint_rescan_fieldvalues_dag",
    "catchup": False,
    "schedule_interval": '0 6 * * *',
    "description": "footprint_rescan_fieldvalues async DAG",
    "default_args": default_dag_args,
    "dagrun_timeout": timedelta(days=30)
}

dag_task_params = [
    {
        "task_id": "footprint_rescan_fieldvalues",
        "python_callable": python_callable,
        "execution_timeout": timedelta(minutes=60 * 12)
    }
]

DAG = BuildDAG().build_dag(dag_params=dag_params, dag_task_params=dag_task_params)
