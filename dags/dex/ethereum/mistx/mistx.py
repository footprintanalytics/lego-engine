from common.common_dex_model import DexModel


class mistxDex(DexModel):
    project_name = 'ethereum_dex_mistx'
    task_name = 'mistx_dex'
    task_liquidity_name = 'mistx_dex_liquidity'
    task_swap_name = 'mistx_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/mistx/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/mistx/swap.sql'


    def build_swap_source_sql(self, match_date_filter: str):
        source = self.get_sql_from_file(
            self.source_swap_sql_file,
            match_date_filter
        )
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
                tx.to_address AS trader_b,
                CAST(token_a_amount_raw AS FLOAT64) AS token_a_amount_raw,
                CAST(token_b_amount_raw AS FLOAT64) AS token_b_amount_raw,
                token_a_address,
                token_b_address,
                exchange_contract_address,
                tx_hash,
                tx.from_address AS tx_from,
                tx.to_address AS tx_to,
                CAST(trace_address AS STRING) AS trace_address
            from (
                {source}
            ) dexs
            inner join `bigquery-public-data.crypto_ethereum.transactions` tx
            on dexs.tx_hash = tx.hash and DATE(tx.block_timestamp) {match_date_filter}
            left join `xed-project-237404.footprint_etl.erc20_tokens` tokena
            on Lower(tokena.contract_address) = Lower(dexs.token_a_address)
            left join `xed-project-237404.footprint_etl.erc20_tokens` tokenb
            on Lower(tokenb.contract_address) = Lower(dexs.token_b_address)
            where ( tx.to_address = '0xfcadf926669e7cad0e50287ea7d563020289ed2c' or tx.to_address = '0xa58f22e0766b3764376c92915ba545d583c19dbc')
            """.format(
            source=source,
            match_date_filter=match_date_filter
        )


if __name__ == '__main__':
    pool = mistxDex()

    daily_sql = pool.build_daily_data_sql()
    # print(daily_sql["add_liquidity_sql"])
    # print(daily_sql["swap_sql"])
    file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql["add_liquidity_sql"])
    file1.write(daily_sql["swap_sql"])

    # history_sql = pool.build_history_data_sql()
    # print(history_sql["add_liquidity_sql"])
    # print(history_sql["swap_sql"])
    # file1 = open('history_sql.sql', 'w')
    # file1.write(history_sql["add_liquidity_sql"])
    # file1.write(history_sql["swap_sql"])
    #
    # print(pool.get_history_table_name())

    pool.run_daily_job()
    # pool.parse_daily_swap_data()

    pool.parse_history_data()
    # pool.create_all_data_view()
    # print(None or 'a')
