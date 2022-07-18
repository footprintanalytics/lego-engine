from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil
import pandas as pd
import os
from config import project_config



class BProtocol(InvestPoolModel1):
    project_name = 'BProtocol'
    task_name = 'bprotocol_pool_transactions'
    execution_time = '40 2 * * *'
    protocol_name = 'B.Protocol'
    protocol_id = 51

    pool_transaction_table_name = 'footprint_flow.origin_transactions_bprotocol'
    token_config = {
        "csv_file_path": 'yield_aggregator/bprotocol/bprotocol_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address'],
        "pool_keys": ['Pool Address'],
        "third_part_token_address": ['Third Part Token Address']
    }

    def parse_config_csv(self):
        dags_folder = project_config.dags_folder
        df = pd.read_csv(os.path.join(dags_folder, self.token_config['csv_file_path']))
        print(df.head())
        self.stake_tokens = self.dataframe_to_list(df, self.token_config['stake_token_keys'])
        self.earn_tokens = self.dataframe_to_list(df, self.token_config['earn_token_keys'])
        self.pool_address = self.dataframe_to_list(df, self.token_config['pool_keys'])
        self.third_part_token_address = self.dataframe_to_list(df,self.token_config['third_part_token_address'])
        self.lqty_contract_address = ['0x0d3abaa7e088c2c82f54b2f47613da438ea8c598','0x54bc9113f1f55cdbdf221daf798dc73614f6d972']

        print('check config stake_tokens ', self.stake_tokens)
        print('check config earn_tokens ', self.earn_tokens)
        print('check config pool_address ', self.pool_address)
        print('check config third_part_token_address ', self.third_part_token_address)
        if self.stake_tokens is None:
            raise Exception('stake_token 不允许为空')

        if self.pool_address is None:
            raise Exception('pool_address 不允许为空')


    def build_classify_function(self):
        """
        所有的分类 method 都是相对于 from_address 来描述的，
        用 from_address + token类型 + to_address 来定义一个转账的类型
        :return:
        """
        return """
        CASE
            WHEN op_user = from_address AND token_address {match_stake_tokens} AND (third_part_token_address {third_part_token_address} or (contract_address {lqty_contract_address} AND to_address {match_contract})) THEN 'deposit'
            WHEN op_user = to_address AND token_address {match_stake_tokens} AND (third_part_token_address {third_part_token_address} or (contract_address {lqty_contract_address} AND from_address {match_contract})) THEN 'withdraw'
            WHEN op_user = to_address AND token_address {match_profit_tokens}  THEN 'profit'
            ELSE 'UnKnow'
        END AS operation,
        """.format(
            match_profit_tokens=SQLUtil.build_match_tokens(self.earn_tokens),
            match_stake_tokens=SQLUtil.build_match_tokens(self.stake_tokens),
            match_contract=SQLUtil.build_match_tokens(self.pool_address),
            lqty_contract_address=SQLUtil.build_match_tokens(self.lqty_contract_address),
            third_part_token_address=SQLUtil.build_match_tokens(self.third_part_token_address),
        )

    def build_classify_transaction_sql(self, source: str):
        """
        先用原生 SQL 实现分类，后面处理复杂逻辑要用 JS UDF
        :return:
        """
        return """
            WITH source_table As (
            {source}
            ),
            filter_table AS (
            select s.*,ss.token_address as third_part_token_address from
                source_table s
                full join (
                select * from source_table where token_address {third_part_token_address}
                )
                ss
                on s.transaction_hash= ss.transaction_hash
            ),
            -- 识别不同的操作
            transactions_op AS (
                SELECT 
                *,
                {classify_function}
                FROM filter_table
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
                s.contract_address,
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
                ON s.token_address = t.contract_address
            )
            SELECT * FROM transactions_token
            """.format(
            source=source,
            classify_function=self.build_classify_function(),
            third_part_token_address=SQLUtil.build_match_tokens(self.third_part_token_address)
        )


if __name__ == '__main__':
    pool = BProtocol()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)
    #
    history_sql = pool.build_history_data_sql()
    print(history_sql)
    file1 = open('history_sql.sql', 'w')
    file1.write(history_sql)

    # print(pool.get_history_table_name())

    # pool.run_daily_job(date_str='2021-08-29')
    # pool.parse_history_data()
    # print(None or 'a')
