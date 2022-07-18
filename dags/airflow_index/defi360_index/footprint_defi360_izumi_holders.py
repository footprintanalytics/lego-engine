from utils.build_dag_util import BuildDAG
from defi360.bigquery_adapter.izumi.izumi_nft_holders import IzumiNftHolders

izumiNftHolders = IzumiNftHolders()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiNftHolders.airflow_dag_params(),
    ops=izumiNftHolders.airflow_steps()
)
