from defi360.bigquery_adapter.izumi.izumi_one_side_withdraw import IzumiOneSideWithdraw
from utils.build_dag_util import BuildDAG

izumiWithdraw = IzumiOneSideWithdraw()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiWithdraw.airflow_dag_params(),
    ops=izumiWithdraw.airflow_steps()
)
