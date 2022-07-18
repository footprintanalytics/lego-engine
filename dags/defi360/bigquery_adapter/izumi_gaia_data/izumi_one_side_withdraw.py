
from defi360.common.bigquery_adapter import BigqueryAdapter
from utils.constant import ENVIRONMENT


class IzumiOneSideWithdrawData(BigqueryAdapter):
    # prod
    project_id = 'gaia-data'
    # 自己确定数据集
    data_set = "gaia"
    task_name = "izumi_one_side_withdraw_gaia_data"
    # time_partitioning_field = None
    execution_time = "56 4 * * *"
    history_date = "2022-05-17"
    schema_name = "defi360/schema/izumi_one_side.json"
    sql_path = "defi360/bigquery_adapter/izumi_gaia_data/izumi_one_side_withdraw.sql"
    history_day = ""

    def get_bigquery_table_name(self):
        table_name = "izumi_one_side_withdraw"
        production_name = "{}.{}.{}".format(self.project_id, self.data_set, table_name)
        test_name = "{}.{}.{}".format(self.project_id_test, self.data_set_test, table_name)
        return production_name if (ENVIRONMENT == 'production' or ENVIRONMENT == 'prod') else test_name


if __name__ == '__main__':
    templateSqlUpload = IzumiOneSideWithdrawData()

    # 跑历史数据
    # templateSqlUpload.load_history_data()

    # 跑当天数据
    # templateSqlUpload.load_daily_data(debug=False)

    # 合并视图 第一次合并即可
    # templateSqlUpload.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
