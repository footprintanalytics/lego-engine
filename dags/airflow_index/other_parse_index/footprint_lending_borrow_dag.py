from external_parsing.lending_asset.load_data_lending_borrow import LendingBorrow
from utils.build_dag_util import BuildDAG

lend = LendingBorrow()

DAG = BuildDAG().build_dag_with_ops(dag_params=lend.airflow_dag_params(), ops=lend.airflow_steps())
