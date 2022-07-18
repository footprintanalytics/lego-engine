from farming.bsc.farming_model import FarmingModel
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery_base,get_schema
from config import project_config
from common.pool_all_data_view_builder import AllDataViewBuilder


class FarmingRewardModel(FarmingModel):
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
                s.receive_address,
                t.symbol as token_symbol,
                s.asset_address as token_address,
                asset_amount / POW(10, t.decimals) AS token_amount,
                asset_amount AS token_amount_raw,
                d.chain AS chain,
                s.pool_id
                FROM source_table s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON LOWER(s.asset_address) = LOWER(t.contract_address)
                left join `xed-project-237404.footprint.defi_protocol_info` d
                on s.protocol_id = d.protocol_id
            )
            SELECT * FROM transactions_token
            """.format(source=source)

    def create_all_data_view(self):
        AllDataViewBuilder.build_all_data_view(
            transactions_table=self.get_daily_table_name(),
            transactions_history_table=self.get_history_table_name(),
            history_date=self.history_date,
            date_column='block_time'
        )
