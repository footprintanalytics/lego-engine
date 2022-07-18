from token_stats.token_holders_daily_stats.token_holders_daily_stats import TokenHoldersDailyStats
from utils.build_dag_util import BuildDAG

ins = TokenHoldersDailyStats()

DAG = BuildDAG().build_dag_with_ops(dag_params=ins.airflow_dag_params(), ops=ins.airflow_steps())
