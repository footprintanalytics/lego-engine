from defi360.bigquery_adapter.izumi.izumi_boost import IzumiBoostUpload
from utils.build_dag_util import BuildDAG

izumiBoost = IzumiBoostUpload()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiBoost.airflow_dag_params(),
    ops=izumiBoost.airflow_steps()
)
