from defi360.python_adapter.ud_defi_pool_ranking import DeFiPoolRinking
from utils.build_dag_util import BuildDAG

deFiPoolRinking = DeFiPoolRinking()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=deFiPoolRinking.airflow_dag_params(),
    ops=deFiPoolRinking.airflow_steps()
)
