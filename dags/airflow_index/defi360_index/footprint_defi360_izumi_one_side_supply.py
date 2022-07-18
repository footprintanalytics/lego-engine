from defi360.bigquery_adapter.izumi.izumi_one_side_supply import IzumiOneSideSupply
from utils.build_dag_util import BuildDAG

izumiSupply = IzumiOneSideSupply()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiSupply.airflow_dag_params(),
    ops=izumiSupply.airflow_steps()
)
