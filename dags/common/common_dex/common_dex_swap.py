
import os

from common.common_pool_model3 import InvestPoolModel3
from common.pool_all_data_view_builder import AllDataViewBuilder
from utils.common import read_file
from utils.constant import PROJECT_PATH
from validation.external_validate.bitquery import BitqueryDexPairAmountValidate, BitqueryDexPairTradesCountValidate, \
    BitqueryDexPairCountValidate
from utils.query_bigquery import query_bigquery
import pydash
from validation.basic_validate.data_continuity import DataContinuityDexTradesVolumeValidate
from datetime import datetime, timedelta

from validation.basic_validate.address_format_validate import AddressFormatValidate
from validation.basic_validate.negative_number_validate import NegativeNumberValidate
from validation.basic_validate.abnormal_number.dex_swap_abnormal_number_validate import DexSwapAbnormalNumberValidate
from validation.basic_validate.token_a_b_validate import TokenABValidate


class CommonDexSwap(InvestPoolModel3):
    def do_import_gsc_to_bigquery(self):
        self.etl.do_import_gsc_to_bigquery(
            project_id=self.project_id,
            schema_name=self.schema_name,
            project_name=self.project_name
        )

    def external_validate(self, date_str):
        self.bitquery_validate(date_str)

    def bitquery_validate(self, date_str):
        protocol_info = self.get_defi_protocol_info()
        table = self.get_daily_table_name()+'_all'
        for info in protocol_info:
            BitqueryDexPairAmountValidate(cfg={
                'validate_date': date_str,
                'validate_table': [table],
                'chain': pydash.get(info, 'chain', 'ethereum').lower(),
                'project': pydash.get(info, 'project'),
                'protocol_id': int(pydash.get(info, 'protocol_id'))
            }).validate()
            BitqueryDexPairCountValidate(cfg={
                'validate_date': date_str,
                'validate_table': [table],
                'chain': pydash.get(info, 'chain', 'ethereum').lower(),
                'project': pydash.get(info, 'project'),
                'protocol_id': int(pydash.get(info, 'protocol_id'))
            }).validate()
            BitqueryDexPairTradesCountValidate(cfg={
                'validate_date': date_str,
                'validate_table': [table],
                'chain': pydash.get(info, 'chain', 'ethereum').lower(),
                'project': pydash.get(info, 'project'),
                'protocol_id': int(pydash.get(info, 'protocol_id'))
            }).validate()

    def get_daily_table_name(self):
        return "{}.{}.{}".format(
            self.project_id,
            self.project_name,
            self.task_name)

    def check_not_none(self):
        pass

    # 基础校验
    def validate(self, validate_table: str = None):
        if not validate_table:
            validate_table = self.get_daily_table_name() + '_all'
        project_data = self.project_name.split('_')
        AddressFormatValidate({
            "validate_field": [
                'token_a_address',
                'token_b_address',
                'trader_a',
                # 'trader_b',
                'exchange_contract_address',
                'tx_from',
                # 'tx_to'
            ],
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        NegativeNumberValidate({
            "validate_field": [
                'token_a_amount',
                'token_b_amount',
                'token_a_amount_raw',
                'token_b_amount_raw'
            ],
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        DexSwapAbnormalNumberValidate({
            "validate_field": [
                'token_a_amount',
                'token_b_amount',
                'token_a_amount_raw',
                'token_b_amount_raw'
            ],
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        DataContinuityDexTradesVolumeValidate({
            "validate_table": [validate_table],
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()
        TokenABValidate({
            "validate_table": validate_table,
            "chain": project_data[0],
            "project": project_data[2]
        }).validate()

    def build_origin_source_sql(self, match_date_filter: str):
        """
        从时间表上把平台相关的交易过滤出来
        :return:
        """
        dags_folder = PROJECT_PATH
        sql_path = os.path.join(dags_folder, self.source_event_sql_file)
        source = read_file(sql_path)
        source = source.replace('{match_date_filter}', match_date_filter)
        sql = """
            select
                DATETIME(dexs.block_time) AS block_time,
                tokena.symbol AS token_a_symbol,
                tokenb.symbol AS token_b_symbol,
                SAFE_DIVIDE(CAST(token_a_amount_raw AS FLOAT64), POW(10, tokena.decimals)) AS token_a_amount,
                SAFE_DIVIDE(CAST(token_b_amount_raw AS FLOAT64), POW(10, tokenb.decimals)) AS token_b_amount,
                project,
                version,
                category,
                protocol_id,
                tx.from_address AS trader_a,
                IFNULL(tx.to_address,'null') AS trader_b,
                CAST(token_a_amount_raw AS FLOAT64) AS token_a_amount_raw,
                CAST(token_b_amount_raw AS FLOAT64) AS token_b_amount_raw,
                lower(token_a_address) as token_a_address,
                lower(token_b_address) as token_b_address,
                lower(exchange_contract_address) as exchange_contract_address,
                tx_hash,
                tx.from_address AS tx_from,
                IFNULL(tx.to_address, 'null')  AS tx_to,
                CAST(trace_address AS STRING) AS trace_address,
                tx.block_number AS block_number
            from (
                {source}
            ) dexs
            inner join `bigquery-public-data.crypto_ethereum.transactions` tx
            on dexs.tx_hash = tx.hash and DATE(tx.block_timestamp) {match_date_filter}
            left join `xed-project-237404.footprint_etl.erc20_tokens` tokena
            on Lower(tokena.contract_address) = Lower(dexs.token_a_address)
            left join `xed-project-237404.footprint_etl.erc20_tokens` tokenb
            on Lower(tokenb.contract_address) = Lower(dexs.token_b_address)
            """.format(
            source=source,
            match_date_filter=match_date_filter
        )
        print('build_origin_source_sql query_string', sql)
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
