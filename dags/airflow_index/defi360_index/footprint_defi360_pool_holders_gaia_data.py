from defi360.contracts_adapter.pool_holders_gaia_data.pool_holders import PoolHoldersData
from utils.build_dag_util import BuildDAG

poolHolders = PoolHoldersData()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=poolHolders.airflow_dag_params(),
    ops=poolHolders.airflow_steps()
)
