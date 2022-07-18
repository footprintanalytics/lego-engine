
from defi360.common.bigquery_adapter import BigqueryAdapter


class IzumiNftDeposit(BigqueryAdapter):
    # 自己确定数据集
    data_set = "izumi"
    task_name = "izumi_nft_deposit"
    time_partitioning_field = "day"
    execution_time = "52 2 * * *"
    history_date = "2022-03-09"
    schema_name = "defi360/schema/izumi_nft_deposit.json"
    sql_path = "defi360/bigquery_adapter/izumi/izumi_nft_deposit.sql"
    # 全量数据需要置为空，默认是100天的
    history_day = ""


if __name__ == '__main__':
    izumiNftDeposit = IzumiNftDeposit()

    # 跑历史数据
    izumiNftDeposit.load_history_data()

    # 跑当天数据
    # izumiNftDeposit.load_daily_data(debug=False)

    # 合并视图 第一次合并即可 4 * 3 * 2 * 3 * 2
    izumiNftDeposit.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
