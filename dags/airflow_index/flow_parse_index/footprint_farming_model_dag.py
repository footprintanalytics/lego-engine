from farming.ethereum.alpha import airflow_steps as alpha_airflow_steps
from farming.ethereum.lido import airflow_steps as lido_airflow_steps
from farming.ethereum.yearn import airflow_steps as yearn_airflow_steps
from farming.bsc.pancakeswap import airflow_steps as pancakeswap_airflow_steps
from farming.ethereum.sushi import airflow_steps as sushi_airflow_steps
from farming.ethereum.harvest import airflow_steps as harvest_airflow_steps
from farming.ethereum.truefi import airflow_steps as truefi_airflow_steps
from farming.ethereum.convex import airflow_steps as convex_airflow_steps
from farming.ethereum.pendle import airflow_steps as pendle_airflow_steps
from farming.bsc.venus import airflow_steps as venus_airflow_steps
from utils.build_dag_util import BuildDAG
from datetime import timedelta, datetime


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


def airflow_dag_params(platform):
    dag_params = {
        "dag_id": "footprint_farming_model_{}_dag".format(platform),
        "catchup": False,
        "schedule_interval": '30 1 * * *',
        "description": "farming_model_{}_dag".format(platform),
        "default_args": {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2021, 8, 20)
        },
        "dagrun_timeout": timedelta(days=30),
        "tags": ['farming_transaction']
    }
    print('dag_params', dag_params)
    return dag_params


all_project = [
    ['alpha', alpha_airflow_steps()],
    ['truefi', truefi_airflow_steps()],
    ['pancakeswap', pancakeswap_airflow_steps()],
    ['lido', lido_airflow_steps()],
    ['yearn', yearn_airflow_steps()],
    ['sushi', sushi_airflow_steps()],
    ['harvest', harvest_airflow_steps()],
    ['convex', convex_airflow_steps()],
    ['pendle', pendle_airflow_steps()],
    ['venus', venus_airflow_steps()],
]

for item in all_project:
    globals()[item[0]] = BuildDAG().build_dag(dag_params=airflow_dag_params(item[0]),
                                            dag_task_params=build_dag_python_operator(item[1]))
