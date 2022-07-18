from gamefi.gamefi_protocol_new_address import GameFiProtocolNewAddress
from utils.build_dag_util import BuildDAG

ins = GameFiProtocolNewAddress()

DAG = BuildDAG().build_dag_with_ops(dag_params=ins.airflow_dag_params(), ops=ins.airflow_steps())
