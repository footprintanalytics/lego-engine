from utils.build_dag_util import BuildDAG
from defi360.python_adapter.business_type_new_address import BusNewAddress

busNewAddress = BusNewAddress()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=busNewAddress.airflow_dag_params(),
    ops=busNewAddress.airflow_steps()
)