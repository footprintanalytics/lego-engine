from gamefi.gamefi_token_transfer.gamefi_token_transfer_bsc import GamefiTokenTransferBsc
from utils.build_dag_util import BuildDAG

ins = GamefiTokenTransferBsc()

# DAG = BuildDAG().build_dag_with_ops(dag_params=ins.airflow_dag_params(), ops=ins.airflow_steps())
