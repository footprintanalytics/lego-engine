from gamefi.gamefi_protocol_daily_stats import GamefiProtocolDailyStats
from utils.build_dag_util import BuildDAG

ins = GamefiProtocolDailyStats()

DAG = BuildDAG().build_dag_with_ops(dag_params=ins.airflow_dag_params(), ops=ins.airflow_steps())
