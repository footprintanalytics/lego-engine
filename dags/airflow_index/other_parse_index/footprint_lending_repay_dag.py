from external_parsing.lending_asset.load_data_lending_repay import LendingRepay
from utils.build_dag_util import BuildDAG

lend = LendingRepay()

DAG = BuildDAG().build_dag_with_ops(dag_params=lend.airflow_dag_params(), ops=lend.airflow_steps())
