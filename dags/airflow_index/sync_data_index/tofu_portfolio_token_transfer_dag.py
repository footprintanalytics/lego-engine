from utils.build_dag_util import BuildDAG
from tofu_portfolio_transfer.token_transfer import token_transfer_config_creator
from tofu_portfolio_transfer.base import BigqeryToMongo

# task = BigqeryToMongo(token_transfer_config_creator())

# deprecated
# DAG = BuildDAG().build_dag_with_ops(dag_params=task.airflow_dag_params(), ops=task.airflow_steps())
