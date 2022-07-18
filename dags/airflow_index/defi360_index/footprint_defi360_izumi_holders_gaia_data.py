from defi360.bigquery_adapter.izumi_gaia_data.izumi_nft_holders import IzumiNftHoldersData
from utils.build_dag_util import BuildDAG

izumiNftHolders = IzumiNftHoldersData()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiNftHolders.airflow_dag_params(),
    ops=izumiNftHolders.airflow_steps()
)
