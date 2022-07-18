
from defi360.common.bigquery_adapter import BigqueryAdapter


class TemplateSqlUpload(BigqueryAdapter):
    # 自己确定数据集
    data_set = "gaia_dao_test"
    task_name = "template_pool_holders"
    time_partitioning_field = "day"
    execution_time = "30 2 * * *"
    history_date = "2022-02-15"
    schema_name = "defi360/schema/template.json"
    sql_path = "defi360/sql_upload/template/izumi_nft_holders.sql"


if __name__ == '__main__':
    templateSqlUpload = TemplateSqlUpload()

    # 跑历史数据
    templateSqlUpload.load_history_data()

    # 跑当天数据
    templateSqlUpload.load_daily_data(templateSqlUpload.history_date)

    # 合并视图 第一次合并即可
    # templateSqlUpload.create_data_view()

    # 校验
    # poolHolders.validate()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
