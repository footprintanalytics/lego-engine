from datetime import timedelta, datetime
from utils.build_dag_util import BuildDAG
from doris_load.auto_create_bigquery_to_doris_task import auto_create_bigquery_to_doris_task


def python_callable():
    auto_create_bigquery_to_doris_task()

default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 1)
}

dag_params = {
    "dag_id": "footprint_auto_create_bigquery_to_doris_dag",
    "catchup": False,
    "schedule_interval": '*/3 * * * *',
    "description": "auto create export data task",
    "default_args": default_dag_args,
    "dagrun_timeout": timedelta(days=14),
    "tags": ["Doris"]
}

dag_task_params = [
    {
        "task_id": "auto_create_bigquery_to_doris_task",
        "python_callable": python_callable,
        "execution_timeout": timedelta(minutes=60 * 12)
    }
]

DAG = BuildDAG().build_dag(dag_params=dag_params, dag_task_params=dag_task_params)
