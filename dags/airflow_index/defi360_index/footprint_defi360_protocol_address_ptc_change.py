from defi360.python_adapter.protocol_address_ptc_change import ProtocolAddressPtcChange
from utils.build_dag_util import BuildDAG

protocolAddressPtcChange = ProtocolAddressPtcChange()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=protocolAddressPtcChange.airflow_dag_params(),
    ops=protocolAddressPtcChange.airflow_steps()
)
