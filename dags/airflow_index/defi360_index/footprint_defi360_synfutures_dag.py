from utils.build_dag_util import BuildDAG
from defi360.bigquery_adapter.SynFutures.SynFutures_cashflow import SynFuturesCashflowUpload

synFuturesCashflowUpload = SynFuturesCashflowUpload()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=synFuturesCashflowUpload.airflow_dag_params(),
    ops=synFuturesCashflowUpload.airflow_steps()
)
