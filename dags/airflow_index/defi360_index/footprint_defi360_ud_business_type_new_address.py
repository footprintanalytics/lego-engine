from utils.build_dag_util import BuildDAG
from defi360.bigquery_adapter.NewAddress.ud_business_type_new_address import BusNewAddressUpload

busNewAddressUpload = BusNewAddressUpload()


DAG = BuildDAG().build_dag_with_ops(
    dag_params=busNewAddressUpload.airflow_dag_params(),
    ops=busNewAddressUpload.airflow_steps()
)