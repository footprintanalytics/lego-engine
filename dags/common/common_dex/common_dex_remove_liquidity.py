from common.common_pool_model3 import InvestPoolModel3
from common.pool_daily_stats_view_builder import DailyStatsViewBuilder
from config import project_config
from utils.common import read_file
from validation.basic_validate.address_format_validate import AddressFormatValidate
from validation.basic_validate.black_hole_address_validate import BlackHoleAddressValidate
from validation.basic_validate.data_continuity.data_continuity_dex_liquidity_volume_validate import \
    DataContinuityDexLiquidityVolumeValidate
from validation.basic_validate.negative_number_validate import NegativeNumberValidate
from validation.basic_validate.abnormal_number.dex_liquidity_abnormal_number_validate import DexLiquidityAbnormalNumberValidate
from common.pool_all_data_view_builder import AllDataViewBuilder
from validation.basic_validate.pool_token_count_validate import PoolTokenCountValidate


class CommonDexRemoveLiquidity(InvestPoolModel3):
    transaction_dataset = 'footprint-blockchain-etl.crypto_ethereum.transactions'

    def do_import_gsc_to_bigquery(self):
        self.etl.do_import_gsc_to_bigquery(
            project_id=self.project_id,
            schema_name=self.schema_name,
            project_name=self.project_name
        )

    def get_daily_table_name(self):
        return "{}.{}.{}".format(
            self.project_id,
            self.project_name,
            self.task_name)

    def check_not_none(self):
        pass

    def build_origin_source_sql(self, match_date_filter: str):
        source = super().build_origin_source_sql(match_date_filter)
        sql = """
            select
                s.*,
                ttx.block_number as block_number
            from ({source}) s
            LEFT JOIN `{transaction_dataset}` ttx ON ttx.hash = s.tx_hash AND DATE(ttx.block_timestamp) {match_date_filter}
        """.format(
            source=source,
            match_date_filter=match_date_filter,
            transaction_dataset=self.transaction_dataset
        )
        return sql

    def build_classify_transaction_sql(self, source: str):
        return source

    def create_all_data_view(self):
        AllDataViewBuilder.build_all_data_view(
            transactions_table=self.get_daily_table_name(),
            transactions_history_table=self.get_history_table_name(),
            history_date=self.history_date,
            date_column='block_time'
        )

    # 基础校验
    def validate(self, validate_table: str = None):
        if not validate_table:
            validate_table = self.get_daily_table_name() + '_all'
        project_data = self.project_name.split('_')
        AddressFormatValidate({
            "validate_field": [
                'liquidity_provider',
                'token_address',
                'exchange_address',
                'tx_from'
            ],
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        BlackHoleAddressValidate({
            "validate_field": [
                'tx_from'
            ],
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        PoolTokenCountValidate({
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        NegativeNumberValidate({
            "validate_field": [
                'token_amount',
                'token_amount_raw'
            ],
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        DexLiquidityAbnormalNumberValidate({
            "validate_table": validate_table,
            "chain": "ethereum",
            "project": project_data[2]
        }).validate()
        DataContinuityDexLiquidityVolumeValidate({
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
