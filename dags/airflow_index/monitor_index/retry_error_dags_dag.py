import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def retry_dags():
    def retry_dags_dag():
        print(airflow.utils.dates.days_ago(1), airflow.utils.dates.days_ago(-1))
        error_dags = airflow.models.dagrun.DagRun.find(
            state='failed',
            execution_start_date=airflow.utils.dates.days_ago(1),
            execution_end_date=airflow.utils.dates.days_ago(-1),
        )
        print("error_dags: ", error_dags)
        success_dags = airflow.models.dagrun.DagRun.find(
            state='success',
            execution_start_date=airflow.utils.dates.days_ago(1),
            execution_end_date=airflow.utils.dates.days_ago(-1),
        )
        running_dags = airflow.models.dagrun.DagRun.find(
            state='running',
            execution_start_date=airflow.utils.dates.days_ago(1),
            execution_end_date=airflow.utils.dates.days_ago(-1),
        )
        if len(error_dags) == 0 or len(success_dags) == 0:
            return
        error_dags = list(map(lambda n: n.dag_id, error_dags))
        success_dags = list(map(lambda n: n.dag_id, success_dags))
        print("error_dags: ", error_dags)
        print("success_dags: ", success_dags)
        exists_error_dags = list(set(error_dags) - set(success_dags))
        if len(running_dags) > 0:
            running_dags = list(map(lambda n: n.dag_id, running_dags))
            print("running_dags: ", running_dags)
            exists_error_dags = list(set(exists_error_dags) - set(running_dags))
        print("exists_error_dags: ", exists_error_dags)
        for d in exists_error_dags:
            subprocess.call(
                f'airflow dags trigger {d}',
                shell=True)

    _dag = DAG(
        'retry_run_dags',
        default_args=default_args,
        description='retry_run_dags dag',
        schedule_interval='*/5 * * * *',
        dagrun_timeout=timedelta(minutes=60))
    task = PythonOperator(
        task_id='retry_dags_dag',
        python_callable=retry_dags_dag,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
        dag=_dag
    )
    return _dag

# deprecated
# DAG = retry_dags()
