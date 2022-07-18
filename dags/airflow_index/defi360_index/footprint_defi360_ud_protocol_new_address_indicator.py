from utils.build_dag_util import BuildDAG
from defi360.python_adapter.ud_protocol_new_address import NewAddress

newAddress = NewAddress()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=newAddress.airflow_dag_params(),
    ops=newAddress.airflow_steps()
)