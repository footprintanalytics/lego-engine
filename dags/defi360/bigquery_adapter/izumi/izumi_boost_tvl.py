
from defi360.common.bigquery_adapter import BigqueryAdapter


class IzumiBoostTVLUpload(BigqueryAdapter):
    # 自己确定数据集
    data_set = "gaia_dao"
    task_name = "izumi_boost_tvl"
    time_partitioning_field = "day"
    execution_time = "30 2 * * *"
    history_date = "2022-02-21"
    schema_name = "defi360/schema/izumi_boost_tvl.json"
    sql_path = "defi360/bigquery_adapter/izumi/izumi_boost_tvl.sql"


if __name__ == '__main__':
    templateSqlUpload = IzumiBoostTVLUpload()

    # 跑历史数据
    # templateSqlUpload.load_history_data()

    # 跑当天数据
    templateSqlUpload.load_daily_data(debug=True, run_date="2022-02-21")

    # 合并视图 第一次合并即可
    # templateSqlUpload.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
