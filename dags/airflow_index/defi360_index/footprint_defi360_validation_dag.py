from utils.build_dag_util import BuildDAG
from defi360.validation import DeFi360Validation

deFi360Validation = DeFi360Validation()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=DeFi360Validation.airflow_dag_params(),
    ops=deFi360Validation.airflow_steps()
)
