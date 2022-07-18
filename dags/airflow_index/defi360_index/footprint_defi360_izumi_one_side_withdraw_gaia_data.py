from defi360.bigquery_adapter.izumi_gaia_data.izumi_one_side_withdraw import IzumiOneSideWithdrawData
from utils.build_dag_util import BuildDAG

izumiWithdraw = IzumiOneSideWithdrawData()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiWithdraw.airflow_dag_params(),
    ops=izumiWithdraw.airflow_steps()
)
