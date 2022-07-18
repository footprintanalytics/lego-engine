from defi360.python_adapter.python_adapter_demo import PythonAdapterDemo
from utils.build_dag_util import BuildDAG

pythonAdapterDemo = PythonAdapterDemo()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=pythonAdapterDemo.airflow_dag_params(),
    ops=pythonAdapterDemo.airflow_steps()
)
