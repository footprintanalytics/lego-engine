
from defi360.common.bigquery_adapter import BigqueryAdapter
from defi360.validation.basic_validate.address_format_validate import AddressFormatValidate
from utils.constant import ENVIRONMENT


class IzumiNftHoldersData(BigqueryAdapter):
    # prod
    project_id = 'gaia-data'
    # 自己确定数据集
    data_set = "gaia"
    task_name = "izumi_nft_holders_gaia_data"
    time_partitioning_field = "day"
    execution_time = "35 2 * * *"
    history_date = "2022-05-17"
    schema_name = "defi360/schema/izumi_nft_holders.json"
    sql_path = "defi360/bigquery_adapter/izumi_gaia_data/izumi_nft_holders.sql"
    history_day = ""
    validate_config = [{
        # 地址格式校验
        "cls": AddressFormatValidate,
        "validate_field": [
            "nft_address",
            "wallet_address"
        ]
    }]

    def get_bigquery_table_name(self):
        table_name = "izumi_nft_holders"
        production_name = "{}.{}.{}".format(self.project_id, self.data_set, table_name)
        test_name = "{}.{}.{}".format(self.project_id_test, self.data_set_test, table_name)
        return production_name if (ENVIRONMENT == 'production' or ENVIRONMENT == 'prod') else test_name


if __name__ == '__main__':
    izumiNftHolders = IzumiNftHoldersData()

    # 跑历史数据
    # izumiNftHolders.load_history_data()

    # izumiNftHolders.validate()

    # 跑当天数据
    # izumiNftHolders.load_daily_data(debug=False)

    # 合并视图 第一次合并即可
    # izumiNftHolders.create_data_view()

    # 增加airflow任务  参照 dags/airflow_index_defi360_index/footprint_defi360_template_dag.py

    print('upload done')
