from utils.build_dag_util import BuildDAG
from defi360.contracts_adapter.pool_holders.pool_holders import PoolHolders

poolHolders = PoolHolders()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=poolHolders.airflow_dag_params(),
    ops=poolHolders.airflow_steps()
)
