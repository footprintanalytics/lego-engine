from defi360.bigquery_adapter.izumi_gaia_data.izumi_one_side_supply import IzumiOneSideSupplyData
from utils.build_dag_util import BuildDAG

izumiSupply = IzumiOneSideSupplyData()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=izumiSupply.airflow_dag_params(),
    ops=izumiSupply.airflow_steps()
)
