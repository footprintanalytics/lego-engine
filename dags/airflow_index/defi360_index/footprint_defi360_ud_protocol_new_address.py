from utils.build_dag_util import BuildDAG
from defi360.bigquery_adapter.NewAddress.ud_protocol_new_address import NewAddressUpload

newAddressUpload = NewAddressUpload()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=newAddressUpload.airflow_dag_params(),
    ops=newAddressUpload.airflow_steps()
)