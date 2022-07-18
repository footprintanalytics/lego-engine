from pool_infos.upload_pools import airflow_steps, airflow_dag_params
from utils.build_dag_util import BuildDAG

DAG = BuildDAG().build_dag_with_ops(dag_params=airflow_dag_params(), ops=airflow_steps())
