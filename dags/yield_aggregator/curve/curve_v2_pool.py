from datetime import timedelta, date

from common.common_pool_model1 import InvestPoolModel1
import pandas as pd
from config import project_config
import os
from utils.sql_util import SQLUtil




class CurveV2Pool(InvestPoolModel1):
    history_date = '2021-09-21'
    project_name = 'Curve'
    task_name = 'curve_v2_pool_transactions'
    execution_time = '30 2 * * *'

    token_config = {
        "csv_file_path": 'yield_aggregator/curve/curve_v2_pools.csv',
        "stake_token_keys": ['token_address'],
        "proxy_contract_address": ['proxy_contract_address'],
        "pool_keys": ['pool_address']
    }
    pool_transaction_table_name = None


    def parse_config_csv(self):
        dags_folder = project_config.dags_folder
        df = pd.read_csv(os.path.join(dags_folder, self.token_config['csv_file_path']))

        self.stake_tokens = self.dataframe_to_list(df, self.token_config['stake_token_keys'])
        self.proxy_contract_address = self.dataframe_to_list(df, self.token_config['proxy_contract_address'])
        self.pool_address = self.dataframe_to_list(df, self.token_config['pool_keys']) + self.proxy_contract_address
        self.simply_pool_address = self.dataframe_to_list(df, self.token_config['pool_keys'])

        print('check config stake_tokens ', self.stake_tokens)
        print('check config proxy_contract_address ', self.proxy_contract_address)
        print('check config pool_address ', self.pool_address)
        if self.stake_tokens is None:
            raise Exception('stake_token 不允许为空')

        if self.pool_address is None:
            raise Exception('pool_address 不允许为空')

        return df

    def build_classify_function(self):
        """
        所有的分类 method 都是相对于 from_address 来描述的，
        用 from_address + token类型 + to_address 来定义一个转账的类型
        :return:
        """
        return """
        CASE
                WHEN ((op_user = from_address AND token_address {match_stake_tokens})  OR (contract_address {proxy_contract_address} and from_address {proxy_contract_address})) AND to_address {match_contract} THEN 'deposit'
            WHEN ((op_user = to_address AND token_address {match_stake_tokens}) OR (contract_address {proxy_contract_address} and to_address {proxy_contract_address})) AND from_address {match_contract} THEN 'withdraw'
            ELSE 'UnKnow'
        END AS operation,
        CASE WHEN contract_address = '0x3993d34e7e99abf6b6f367309975d1360222d446' THEN '0xd51a44d3fae010294c616388b506acda1bfaae46'
        ELSE contract_address
        END AS real_contract_address,
        """.format(
            match_stake_tokens=SQLUtil.build_match_tokens(self.stake_tokens),
            match_contract=SQLUtil.build_match_tokens(self.simply_pool_address),
            proxy_contract_address=SQLUtil.build_match_tokens(self.proxy_contract_address),
        )

    def build_classify_transaction_sql(self, source: str):
        """
        先用原生 SQL 实现分类，后面处理复杂逻辑要用 JS UDF
        :return:
        """
        return """
            WITH source_table AS (
                {source}
            ),
            -- 识别不同的操作
            transactions_op AS (
                SELECT 
                *,
                {classify_function}
                FROM source_table
            ),
            -- 过滤未知的操作
            transactions_filter AS (
                SELECT 
                *
                FROM transactions_op
                WHERE operation != 'UnKnow' 
            ),

            -- 连 tokoen 表
            transactions_token AS(
                SELECT
                s.transaction_hash,
                s.block_timestamp,
                s.op_user,
                s.real_contract_address as contract_address,
                s.gas,
                s.gas_price,
                s.from_address,
                s.to_address,
                s.token_address,
                t.symbol AS token_symbol,
                s.operation,
                value / POW(10, t.decimals) AS value,
                FROM transactions_filter s
                LEFT JOIN `xed-project-237404.footprint_etl.erc20_tokens` t
                ON s.token_address = LOWER(t.contract_address)
            )
            SELECT * FROM transactions_token
            """.format(
            source=source,
            classify_function=self.build_classify_function()
        )



    # def parse_config_csv(self):

if __name__ == '__main__':
    pool = CurveV2Pool()

    # daily_sql = pool.build_daily_data_sql()
    # print(daily_sql)
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    # history_sql = pool.build_history_data_sql()
    # print(history_sql)
    # file1 = open('history_sql.sql', 'w')
    # file1.write(history_sql)

    # etl_by_date()
    #
    # print(pool.get_history_table_name())

    # pool.run_daily_job()
    pool.create_all_data_view()

    # pool.create_pool_daily_view()
    # pool.parse_history_data()
    # print(None or 'a')

    # with open('./curve_v1_pools.json', 'r') as f:
    #     data = json.load(f)
    #     df = pd.DataFrame(data, columns=['pool_address', 'pool_name', 'token_symbol', 'token_address',
    #                                      'decimals'])
    #     print(data)
    #     df.to_csv('./curve_v1_pools.csv', index=False)
