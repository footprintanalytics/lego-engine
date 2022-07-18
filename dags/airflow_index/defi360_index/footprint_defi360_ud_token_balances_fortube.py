from defi360.contracts_adapter.token_balances.fortube.ud_token_balances_fortube import TokenBalancesFortube
from utils.build_dag_util import BuildDAG

tokenBalancesFortube = TokenBalancesFortube()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=tokenBalancesFortube.airflow_dag_params(),
    ops=tokenBalancesFortube.airflow_steps()
)
