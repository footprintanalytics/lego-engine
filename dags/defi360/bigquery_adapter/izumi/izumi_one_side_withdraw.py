
from defi360.common.bigquery_adapter import BigqueryAdapter


class IzumiOneSideWithdraw(BigqueryAdapter):
    # 自己确定数据集
    data_set = "gaia_dao"
    task_name = "izumi_one_side_withdraw"
    # time_partitioning_field = None
    execution_time = "17 4 * * *"
    history_date = "2022-04-01"
    schema_name = "defi360/schema/izumi_one_side.json"
    sql_path = "defi360/bigquery_adapter/izumi/izumi_one_side_withdraw.sql"


if __name__ == '__main__':
    templateSqlUpload = IzumiOneSideWithdraw()

    # 跑历史数据
    templateSqlUpload.load_history_data()

    # 跑当天数据
    # templateSqlUpload.load_daily_data(debug=False)

    # 合并视图 第一次合并即可
    # templateSqlUpload.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
