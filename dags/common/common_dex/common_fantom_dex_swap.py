
import os

from .common_dex_swap import CommonDexSwap
from utils.common import read_file
from utils.constant import PROJECT_PATH


class CommonFantomDexSwap(CommonDexSwap):

    # 生成swap的sql
    def build_origin_source_sql(self, match_date_filter: str):
        dags_folder = PROJECT_PATH
        sql_path = os.path.join(dags_folder, self.source_event_sql_file)
        source = read_file(sql_path)
        source = source.replace('{match_date_filter}', match_date_filter)

        return """
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
                IFNULL(tx.to_address,'null')  AS trader_b,
                CAST(token_a_amount_raw AS FLOAT64) AS token_a_amount_raw,
                CAST(token_b_amount_raw AS FLOAT64) AS token_b_amount_raw,
                token_a_address,
                token_b_address,
                exchange_contract_address,
                tx_hash,
                tx.from_address AS tx_from,
                IFNULL(tx.to_address,'null')  AS tx_to,
                CAST(trace_address AS STRING) AS trace_address,
                tx.block_number AS block_number
            from (
                {source}
            ) dexs
            inner join `footprint-blockchain-etl.crypto_fantom.transactions` tx
            on dexs.tx_hash = tx.hash and DATE(tx.block_timestamp) {match_date_filter}
            left join `xed-project-237404.footprint_etl.fantom_erc20_tokens` tokena
            on Lower(tokena.contract_address) = Lower(dexs.token_a_address)
            left join `xed-project-237404.footprint_etl.fantom_erc20_tokens` tokenb
            on Lower(tokenb.contract_address) = Lower(dexs.token_b_address)
            where tx.to_address is not null
            """.format(
            source=source,
            match_date_filter=match_date_filter
        )
