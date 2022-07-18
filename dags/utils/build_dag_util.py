from datetime import timedelta

from airflow import models
from airflow.operators.python_operator import PythonOperator

"""dag_params 对dag的参数进行限制"""
"""task_params 对task 的参数进行限制"""


def build_task(dag, task_param):
    task_id = task_param['task_id']
    python_callable = task_param['python_callable']
    execution_timeout = task_param['execution_timeout']
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        execution_timeout=execution_timeout,
        dag=dag
    )


class BuildDAG():

    def __init__(self):
        self.task = None

    def build_dag(self, **kwargs):
        dag_params = kwargs.get('dag_params')
        dag_task_params = kwargs.get('dag_task_params')

        dag_id = dag_params.get('dag_id')
        catchup = dag_params.get('catchup')
        schedule_interval = dag_params.get('schedule_interval')
        description = dag_params.get('description')
        default_args = dag_params.get('default_args')
        tags = dag_params.get('tags')

        dag = models.DAG(
            dag_id=dag_id,
            catchup=catchup,
            schedule_interval=schedule_interval,
            default_args=default_args,
            description=description,
            tags=tags if tags else []
        )
        for dag_task_param in dag_task_params:
            task = self.build_task(dag, dag_task_param)
            if not self.task:
                self.task = task
            else:
                self.task = self.task >> task
        return dag

    def build_task(self, dag, task_param):
        task_id = task_param['task_id']
        python_callable = task_param['python_callable']
        execution_timeout = task_param['execution_timeout']
        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            execution_timeout=execution_timeout,
            dag=dag
        )

    def build_dag_with_ops(self, dag_params, ops):
        dag_task_params = self.build_dag_python_operator(ops)
        return self.build_dag(dag_params=dag_params, dag_task_params=dag_task_params)

    def build_dag_python_operator(self, steps: list):
        def build_task_params(method):
            return {
                "task_id": method.__name__,
                "python_callable": method,
                "execution_timeout": timedelta(days=30)
            }

        dag_task_params = list(map(build_task_params, steps))
        print('python_operator ', dag_task_params)
        return dag_task_params
