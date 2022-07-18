from token_stats.token_balance.update_token_balance import TokenBalance
from utils.build_dag_util import BuildDAG

token_balance = TokenBalance()

DAG = BuildDAG().build_dag_with_ops(dag_params=token_balance.airflow_dag_params(), ops=token_balance.airflow_steps())
