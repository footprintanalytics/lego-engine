from defi360.python_adapter.defi_turnover_tvl import DeFiTurnoverTVL
from utils.build_dag_util import BuildDAG

deFiTurnoverTVL = DeFiTurnoverTVL()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=deFiTurnoverTVL.airflow_dag_params(),
    ops=deFiTurnoverTVL.airflow_steps()
)
