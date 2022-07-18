from defi360.python_adapter.protocol_daily_stats import ProtocolDailyStats
from utils.build_dag_util import BuildDAG

protocolDailyStats = ProtocolDailyStats()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=protocolDailyStats.airflow_dag_params(),
    ops=protocolDailyStats.airflow_steps()
)
