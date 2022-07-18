from defi360.python_adapter.monthly_protocol_address_retention import MonthlyProtocolAddressRetention
from utils.build_dag_util import BuildDAG

monthlyProtocolAddressRetention = MonthlyProtocolAddressRetention()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=monthlyProtocolAddressRetention.airflow_dag_params(),
    ops=monthlyProtocolAddressRetention.airflow_steps()
)
