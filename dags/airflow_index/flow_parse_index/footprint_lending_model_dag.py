from utils.build_dag_util import BuildDAG, build_task
from datetime import timedelta, datetime
from airflow import models

from lending.ethereum.aave import aave_business_list
from lending.ethereum.maple import maple_business_list
from lending.ethereum.compound import compound_business_list
from lending.ethereum.bzx import bzx_business_list
from lending.ethereum.dforce import dforce_business_list
from lending.ethereum.truefi import truefi_business_list
from lending.ethereum.cream import cream_business_list
from lending.ethereum.raricapital import raricapital_business_list
from lending.bsc.venus import venus_business_list
from lending.bsc.dforce import dforce_business_list as bsc_dforce_business_list
from lending.polygon.aave import aave_business_list as polygon_aave_business_list
from lending.avalanche.aave import aave_business_list as avalanche_aave_business_list


def build_dag_python_operator(steps: list):
    def build_task_params(type, method):
        return {
            "task_id": '{}_{}'.format(type, method.__name__),
            "python_callable": method,
            "execution_timeout": timedelta(days=30)
        }

    dag_task_params = list(map(lambda n: build_task_params(n.get('type'), n.get('func')), steps))
    print('python_operator ', dag_task_params)
    return dag_task_params


def create_dag(task_name, schedule_interval, business_list):
    dag = models.DAG(
        dag_id=f'footprint_lending_model_{task_name}_dag',
        schedule_interval=schedule_interval,
        start_date=datetime(2021, 11, 29),
        tags=['lending'],
        catchup=False,
        default_args={'owner': 'airflow', 'depends_on_past': False, 'retries': 5, 'retry_delay': timedelta(minutes=5),
                      'start_date': datetime(2021, 8, 20)},
        dagrun_timeout=timedelta(days=1)
    )

    for business in business_list:
        steps = business.airflow_steps()
        operator_params = list(map(lambda n: {'type': business.task_name.split('_')[-1], 'func': n}, steps))

        task = None
        for dag_task_param in build_dag_python_operator(steps=operator_params):
            dag_task = build_task(dag=dag, task_param=dag_task_param)
            if not task:
                task = dag_task
            else:
                task = task >> dag_task

    return dag


all_project = [
    aave_business_list,
    compound_business_list,
    cream_business_list,
    truefi_business_list,
    bzx_business_list,
    venus_business_list,
    dforce_business_list,
    bsc_dforce_business_list,
    maple_business_list,
    raricapital_business_list,
    polygon_aave_business_list,
    avalanche_aave_business_list
]

for project in all_project:
    if len(project) > 0:
        one_business = project[0]
        globals()[one_business.project_name] = create_dag(task_name='_'.join(one_business.task_name.split('_')[:-1]),
                                                          schedule_interval=one_business.execution_time,
                                                          business_list=project)
