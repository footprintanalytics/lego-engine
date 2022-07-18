from utils.build_dag_util import BuildDAG
from defi360.contracts_adapter.token_balances.token_balances_dodo import TokenBalancesDODO

tokenBalancesDODO = TokenBalancesDODO()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=tokenBalancesDODO.airflow_dag_params(),
    ops=tokenBalancesDODO.airflow_steps()
)
