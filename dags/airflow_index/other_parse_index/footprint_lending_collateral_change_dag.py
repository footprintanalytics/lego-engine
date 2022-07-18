from external_parsing.lending_asset.load_data_lending_collateral_change import LendingCollateralChange
from utils.build_dag_util import BuildDAG

lend = LendingCollateralChange()

DAG = BuildDAG().build_dag_with_ops(dag_params=lend.airflow_dag_params(), ops=lend.airflow_steps())
