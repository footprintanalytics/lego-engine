from common.common_pool_model3 import InvestPoolModel3
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery_base, get_schema
from config import project_config
from utils.constant import CHAIN
from common.pool_all_data_view_builder import AllDataViewBuilder
from validation.basic_validate.abnormal_number.farming_abnormal_number_validate import FarmingAbnormalNumberValidate
from validation.basic_validate.address_format_validate import AddressFormatValidate
from validation.basic_validate.data_continuity.data_continuity_farming_volume_validate import \
    DataContinuityFarmingVolumeValidate
from validation.basic_validate.negative_number_validate import NegativeNumberValidate

bucket_name = project_config.bigquery_bucket_name


class FarmingModel(InvestPoolModel3):
    project_id = 'footprint-etl'
    chain = CHAIN['BSC']

    def __init__(self):
        if 'bsc_' not in self.task_name:
            self.task_name = 'bsc_' + self.task_name
        if 'bsc_' not in self.project_name:
            self.project_name = 'bsc_' + self.project_name
        super().__init__()

    def build_classify_transaction_sql(self, source: str):
        """
        先用原生 SQL 实现分类，后面处理复杂逻辑要用 JS UDF
        :return:
        """
        return """
            WITH source_table AS (
                {source}
            ),
            -- 连 tokoen 表
            transactions_token AS(
                SELECT
                s.type,
                s.project,
                s.version,
                s.protocol_id,
                d.protocol_slug,
                s.block_number,
                s.block_timestamp as block_time,
                s.transaction_hash as tx_hash,
                s.log_index,
                s.contract_address,
                s.operator,
                t.symbol as asset_symbol,
                s.asset_address as token_address,
                asset_amount / POW(10, t.decimals) AS token_amount,
                asset_amount AS token_amount_raw,
                d.chain AS chain,
                s.pool_id
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.bsc_erc20_tokens` t
                ON LOWER(s.asset_address) = LOWER(t.contract_address)
                left join `xed-project-237404.footprint.defi_protocol_info` d
                on s.protocol_id = d.protocol_id
            )
            SELECT * FROM transactions_token
            """.format(source=source)

    def do_import_gsc_to_bigquery(self):
        """
               重写 do_import_gsc_to_bigquery 方法，将data_base 定义成footprint_lending_etl
        """
        uri = "gs://{bucket_name}/{gsc_folder}/*.csv".format(bucket_name=bucket_name, gsc_folder=self.task_name)
        import_gsc_to_bigquery_base(schema=get_schema('farming_transaction'), uri=uri,
                                    bigquery_table_name=self.get_daily_table_name())

    def get_daily_table_name(self):
        return '{}.bsc_farming_{}.{}'.format(self.project_id, self.dataset_name_prefix, self.task_name)

    def create_all_data_view(self):
        AllDataViewBuilder.build_all_data_view(
            transactions_table=self.get_daily_table_name(),
            transactions_history_table=self.get_history_table_name(),
            history_date=self.history_date,
            date_column='block_time'
        )

    # 基础校验
    def validate(self, validate_table: str = None, interval_days: int = 1):
        if not validate_table:
            validate_table = self.get_daily_table_name() + "_all"
        AddressFormatValidate({
            "validate_field": [
                'operator',
                'token_address',
                'contract_address'
            ],
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        NegativeNumberValidate({
            "validate_field": ['token_amount'],
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        FarmingAbnormalNumberValidate({
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        DataContinuityFarmingVolumeValidate({
            "interval_days": interval_days,
            "validate_table": [validate_table],
            "chain": self.chain,
            "project": self.project_name
        }).validate()
