from gamefi.gamefi_protocol_transcation.gamefi_protocol_transaction_avalanche import GamefiProtocolTransactionAvalanche
from utils.build_dag_util import BuildDAG

ins = GamefiProtocolTransactionAvalanche()

DAG = BuildDAG().build_dag_with_ops(dag_params=ins.airflow_dag_params(), ops=ins.airflow_steps())
