from utils.build_dag_util import BuildDAG
from defi360.bigquery_adapter.izumi.izumi_nft_deposit import IzumiNftDeposit

izumiNftDeposit = IzumiNftDeposit()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiNftDeposit.airflow_dag_params(),
    ops=izumiNftDeposit.airflow_steps()
)
