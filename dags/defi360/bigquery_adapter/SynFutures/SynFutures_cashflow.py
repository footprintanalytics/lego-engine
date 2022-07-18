
from defi360.common.bigquery_adapter import BigqueryAdapter


class SynFuturesCashflowUpload(BigqueryAdapter):
    # 自己确定数据集
    data_set = "gaia_dao"
    task_name = "SynFutures_b70284c2-6817-43ae-91c9-fac94888c66f"
    time_partitioning_field = "block_timestamp"
    execution_time = "0 3 * * *"
    history_date = "2022-02-19"
    schema_name = "defi360/schema/SynFutures_cashflow.json"
    sql_path = "defi360/bigquery_adapter/SynFutures/SynFutures_cashflow.sql"


if __name__ == '__main__':
    synFuturesCashflowUpload = SynFuturesCashflowUpload()

    # 跑历史数据
    # synFuturesCashflowUpload.load_history_data()

    # 跑当天数据
    synFuturesCashflowUpload.load_daily_data(debug=True)

    # 合并视图 第一次合并即可
    # synFuturesCashflowUpload.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
