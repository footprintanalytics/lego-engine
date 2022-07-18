from common.common_pool_model1 import InvestPoolModel1
import pandas as pd
from utils.sql_util import SQLUtil
import os
from config import project_config


class Pendle(InvestPoolModel1):
    project_name = 'Pendle'
    task_name = 'pendle_pool_transactions'
    execution_time = '35 2 * * *'

    pool_transaction_table_name = 'footprint_flow.origin_transactions_pendle'
    token_config = {
        "csv_file_path": 'yield_aggregator/pendle/pendle_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "temporary_storage_address": ['Temporary Storage Address'],
        "earn_token_keys": ['Earn Token 1 Address'],
        "pool_keys": ['Pool Address']
    }

    def parse_config_csv(self):
        dags_folder = project_config.dags_folder
        df = pd.read_csv(os.path.join(dags_folder, self.token_config['csv_file_path']))
        self.stake_tokens = self.dataframe_to_list(df, self.token_config['stake_token_keys'])
        self.earn_tokens = self.dataframe_to_list(df, self.token_config['earn_token_keys'])
        self.pool_address = self.dataframe_to_list(df, self.token_config['pool_keys'])
        self.temporary_storage_address = self.dataframe_to_list(df,self.token_config['temporary_storage_address']) + self.pool_address

        print('check config stake_tokens ', self.stake_tokens)
        print('check config pool_address ', self.pool_address)
        print('check config temporary_storage_address ', self.temporary_storage_address)
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
            WHEN op_user = from_address AND token_address {match_stake_tokens} AND to_address {temporary_storage_address} THEN 'deposit'
            WHEN op_user = to_address AND token_address {match_stake_tokens} AND from_address {temporary_storage_address} THEN 'withdraw'
            ELSE 'UnKnow'
        END AS operation,
        """.format(
            # match_profit_tokens=SQLUtil.build_match_tokens(self.earn_tokens),
            match_stake_tokens=SQLUtil.build_match_tokens(self.stake_tokens),
            match_contract=SQLUtil.build_match_tokens(self.pool_address),
            # lqty_contract_address=SQLUtil.build_match_tokens(self.lqty_contract_address),
            temporary_storage_address=SQLUtil.build_match_tokens(self.temporary_storage_address)
        )



if __name__ == '__main__':
    pool = Pendle()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)

    history_sql = pool.build_history_data_sql()
    print(history_sql)
    file1 = open('history_sql.sql', 'w')
    file1.write(history_sql)

    # print(pool.get_history_table_name())

    # pool.run_daily_job(date_str='2021-08-29')
    # pool.create_all_data_view()

    # pool.create_pool_daily_view()
    # pool.parse_history_data()
    # print(None or 'a')
