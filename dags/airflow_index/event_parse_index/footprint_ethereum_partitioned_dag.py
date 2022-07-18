from utils.build_dag_util import BuildDAG
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import models


def python_callable_stage_1():
    print('mock partition success')
    return 'done'


def python_callable_stage_2():
    print('do stage 2')


def python_callable_stage_3():
    print('do stage 2')


default_dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 7, 1)
}

dag_params = {
    "dag_id": "ethereum_partition_dag",
    "catchup": False,
    "schedule_interval": '35 11 * * *',
    "description": "ethereum_partition_dag",
    "default_args": default_dag_args,
    "dagrun_timeout": timedelta(days=30)
}

DAG = models.DAG(
    dag_id='ethereum_partition_dag',
    catchup=False,
    default_args=default_dag_args,
    dagrun_timeout=timedelta(days=30),
    schedule_interval='35 11 * * *',
)

task_partition = PythonOperator(
    task_id='partition_logs',
    python_callable=python_callable_stage_1,
    provide_context=True,
    dag=DAG
)

task_done = BashOperator(
    task_id='done',
    bash_command='echo done',
    dag=DAG
)

task_partition >> task_done
