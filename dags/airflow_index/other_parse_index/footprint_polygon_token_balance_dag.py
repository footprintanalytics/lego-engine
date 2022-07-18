from token_stats.token_balance.update_polygon_token_balance import PolygonTokenBalance
from utils.build_dag_util import BuildDAG

token_balance = PolygonTokenBalance()

DAG = BuildDAG().build_dag_with_ops(dag_params=token_balance.airflow_dag_params(), ops=token_balance.airflow_steps())
