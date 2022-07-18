from defi360.python_adapter.izumi_one_side_tvl import IzumiOneSideTvl
from utils.build_dag_util import BuildDAG

deFiPoolRinking = IzumiOneSideTvl()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=deFiPoolRinking.airflow_dag_params(),
    ops=deFiPoolRinking.airflow_steps()
)
