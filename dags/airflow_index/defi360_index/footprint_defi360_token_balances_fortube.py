from defi360.contracts_adapter.token_balances.fortube.token_balances_fortube import TokenBalancesFortube
from utils.build_dag_util import BuildDAG

tokenBalancesFortube = TokenBalancesFortube()
tokenBalancesFortube.project_id = 'gaia-dao'
tokenBalancesFortube.data_set = 'gaia_dao'

DAG = BuildDAG().build_dag_with_ops(
    dag_params=tokenBalancesFortube.airflow_dag_params(),
    ops=tokenBalancesFortube.airflow_steps()
)
