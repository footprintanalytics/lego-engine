from token_stats.address_token_daily_stats.address_token_daily_stats import AddressTokenDailyStats
from utils.build_dag_util import BuildDAG

ins = AddressTokenDailyStats()

DAG = BuildDAG().build_dag_with_ops(dag_params=ins.airflow_dag_params(), ops=ins.airflow_steps())
