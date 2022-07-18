# from datetime import timedelta, datetime
# from utils.build_dag_util import BuildDAG
# from doris_load.check_bigquery_doris_row_count import check_bigquery_doris_row_count
#
#
# def python_callable():
#     check_bigquery_doris_row_count()
#
#
# default_dag_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'retries': 0,
#     'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2022, 5, 1)
# }
#
# dag_params = {
#     "dag_id": "footprint_monitor_bigquery_doris_row_count_dag",
#     "catchup": False,
#     "schedule_interval": '*/60 * * * *',
#     "description": "monitor_bigquery_doris_row_count dag",
#     "default_args": default_dag_args,
#     "dagrun_timeout": timedelta(days=14),
#     "tags": ["Doris"]
# }
#
# dag_task_params = [
#     {
#         "task_id": "monitor_bigquery_doris_row_count",
#         "python_callable": python_callable,
#         "execution_timeout": timedelta(minutes=60 * 12)
#     }
# ]
#
# DAG = BuildDAG().build_dag(dag_params=dag_params, dag_task_params=dag_task_params)
