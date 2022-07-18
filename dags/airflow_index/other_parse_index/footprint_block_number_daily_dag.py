from transactions.block_number_daily import BlockNumberDaily
from utils.build_dag_util import BuildDAG
from datetime import timedelta, datetime
from utils.monitor import send_to_slack


def python_callable():
    BlockNumberDaily().run_daily_job()


def on_failure_callback(context):
    message = f'footprint block number daily 任务执行失败，需要人介入'
    send_to_slack(message)


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 7, 1),
    'on_failure_callback': on_failure_callback
}

dag_params = {
    "dag_id": "footprint_block_number_daily",
    "catchup": False,
    "schedule_interval": '0 6 * * *',
    "description": "get block number daily stats info DAG",
    "default_args": default_dag_args,
    "dagrun_timeout": timedelta(days=30)
}

dag_task_params = [
    {
        "task_id": "footprint_block_number_daily",
        "python_callable": python_callable,
        "execution_timeout": timedelta(minutes=60 * 12)
    }
]

DAG = BuildDAG().build_dag(dag_params=dag_params, dag_task_params=dag_task_params)
