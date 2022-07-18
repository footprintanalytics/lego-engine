from defi360.python_adapter.defi_pool_ranking import DeFiPoolRinking
from utils.build_dag_util import BuildDAG

deFiPoolRinking = DeFiPoolRinking()
deFiPoolRinking.project_id = 'gaia-data'
deFiPoolRinking.data_set = 'gaia'
deFiPoolRinking.sql_path = 'defi360/python_adapter/defi_pool_ranking_v2.sql'

DAG = BuildDAG().build_dag_with_ops(
    dag_params=deFiPoolRinking.airflow_dag_params(),
    ops=deFiPoolRinking.airflow_steps()
)
