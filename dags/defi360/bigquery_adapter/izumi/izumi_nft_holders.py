
from defi360.common.bigquery_adapter import BigqueryAdapter
from defi360.validation.basic_validate.address_format_validate import AddressFormatValidate


class IzumiNftHolders(BigqueryAdapter):
    # 自己确定数据集
    data_set = "izumi"
    task_name = "izumi_nft_holders"
    time_partitioning_field = "day"
    execution_time = "35 2 * * *"
    history_date = "2022-03-09"
    schema_name = "defi360/schema/izumi_nft_holders.json"
    sql_path = "defi360/bigquery_adapter/izumi/izumi_nft_holders.sql"
    history_day = 100
    validate_config = [{
        # 地址格式校验
        "cls": AddressFormatValidate,
        "validate_field": [
            "nft_address",
            "wallet_address"
        ]
    }]


if __name__ == '__main__':
    izumiNftHolders = IzumiNftHolders()

    # 跑历史数据
    # izumiNftHolders.load_history_data()

    # izumiNftHolders.validate()

    # 跑当天数据
    # izumiNftHolders.load_daily_data(debug=False)

    # 合并视图 第一次合并即可
    # izumiNftHolders.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
