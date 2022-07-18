from common.common_pool_model3 import InvestPoolModel3
from utils.constant import BUSINESS_SECOND_TYPE, CHAIN
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery_base,get_schema
from config import project_config
from common.pool_all_data_view_builder import AllDataViewBuilder
from validation.basic_validate.data_continuity import DataContinuityLendingVolumeValidate
from validation.basic_validate.address_format_validate import AddressFormatValidate
from validation.basic_validate.negative_number_validate import NegativeNumberValidate
from validation.basic_validate.abnormal_number import (
    LendingAbnormalNumberValidateLiquidation,
    LendingAbnormalNumberValidate
)

bucket_name = project_config.bigquery_bucket_name


class MintingModel(InvestPoolModel3):
    project_id = 'footprint-etl'
    chain = None
    schema_name = 'minting'

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
                s.block_time,
                s.tx_hash,
                s.log_index,
                s.contract_address,
                s.operator,
                t.symbol as token_symbol,
                s.token_address,
                token_amount_raw / POW(10, t.decimals) AS token_amount,
                token_amount_raw,
                d.chain AS chain,
                s.pool_id
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.{chain}erc20_tokens` t
                ON LOWER(s.token_address) = LOWER(t.contract_address)
                left join `xed-project-237404.footprint.defi_protocol_info` d
                on s.protocol_id = d.protocol_id
            )
            SELECT * FROM transactions_token
            """.format(source=source, chain='{}_'.format(self.chain) if self.chain != CHAIN['ETHEREUM'] else '')

    def do_import_gsc_to_bigquery(self):
        """
               重写 do_import_gsc_to_bigquery 方法，将data_base 定义成footprint_minting_etl
        """
        uri = "gs://{bucket_name}/{gsc_folder}/*.csv".format(bucket_name=bucket_name, gsc_folder=self.task_name)
        import_gsc_to_bigquery_base(schema=get_schema('minting_transaction'), uri=uri,
                                    bigquery_table_name=self.get_daily_table_name(),name=self.schema_name)

    def get_daily_table_name(self):
        return '{}.{}_minting_{}.{}'.format(self.project_id, self.chain, self.dataset_name_prefix, self.task_name)

    def create_all_data_view(self):
        AllDataViewBuilder.build_all_data_view(
            transactions_table=self.get_daily_table_name(),
            transactions_history_table=self.get_history_table_name(),
            history_date=self.history_date,
            date_column='block_time'
        )

    def validate(self, validate_table: str = None):
        if not validate_table:
            validate_table = self.get_daily_table_name() + "_all"
        AddressFormatValidate({
            "validate_field": [
                'operator',
                'token_address'
            ],
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()

        NegativeNumberValidate({
            "validate_field": [
                'token_amount_raw'
            ],
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        LendingAbnormalNumberValidate({
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        DataContinuityLendingVolumeValidate({
            "validate_table": [validate_table],
            "chain": self.chain,
            "project": self.project_name
        }).validate()


class MintingBorrowModel(MintingModel):
    pass


class MintingRepayModel(MintingModel):
    pass


class MintingSupplyModel(MintingModel):
    pass


class MintingWithdrawModel(MintingModel):
    pass


class MintingLiquidationModel(MintingModel):
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
                    transactions_token AS (
                        SELECT
                        s.type,
                        s.project,
                        s.version,
                        s.protocol_id,
                        d.protocol_slug,
                        s.block_number,
                        s.block_time,
                        s.tx_hash,
                        s.log_index,
                        s.contract_address,
                        -- 借款人
                        s.borrower,
                        t.symbol as token_collateral_symbol,
                        s.token_collateral_address,
                        case when s.project = 'MakerDAO' then (s.token_collateral_amount_raw / POW(10, 18)) else (s.token_collateral_amount_raw / POW(10, t.decimals)) end AS token_collateral_amount,
                        s.token_collateral_amount_raw,

                        -- 清算人
                        s.liquidator,
                        tr.symbol as repay_token_symbol,
                        s.repay_token_address,
                        s.repay_token_amount_raw / POW(10, tr.decimals) AS repay_token_amount,
                        s.repay_token_amount_raw,

                        d.chain AS chain,
                        s.pool_id
                        FROM source_table s
                        LEFT JOIN `xed-project-237404.footprint_etl.{chain}erc20_tokens` t
                        ON LOWER(s.token_collateral_address) = LOWER(t.contract_address)
                        LEFT JOIN `xed-project-237404.footprint_etl.{chain}erc20_tokens` tr
                        ON LOWER(s.repay_token_address) = LOWER(tr.contract_address)
                        left join `xed-project-237404.footprint.defi_protocol_info` d
                        on s.protocol_id = d.protocol_id
                    )
                    SELECT * FROM transactions_token
                    """.format(source=source, chain='{}_'.format(self.chain) if self.chain != CHAIN['ETHEREUM'] else '')

    def do_import_gsc_to_bigquery(self):
        """
        重写 do_import_gsc_to_bigquery 方法，将data_base 定义成footprint_minting_etl
        """
        uri = "gs://{bucket_name}/{gsc_folder}/*.csv".format(bucket_name=bucket_name, gsc_folder=self.task_name)
        import_gsc_to_bigquery_base(
            schema=get_schema('minting_liquidation'), uri=uri, bigquery_table_name=self.get_daily_table_name())

    def validate(self, validate_table: str = None):
        if not validate_table:
            validate_table = self.get_daily_table_name() + "_all"
        AddressFormatValidate({
            "validate_field": [
                'borrower',
                'liquidator',
                'token_collateral_address',
                'repay_token_address'
            ],
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()

        NegativeNumberValidate({
            "validate_field": [
                'token_collateral_amount',
                'repay_token_amount'
            ],
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        LendingAbnormalNumberValidateLiquidation({
            "validate_table": validate_table,
            "chain": self.chain,
            "project": self.project_name
        }).validate()
        # 清算不做连续性校验
