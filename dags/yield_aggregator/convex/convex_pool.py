from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil

class ConvexPool(InvestPoolModel1):
    project_name = 'Convex'
    task_name = 'convex_pool_transactions'
    execution_time = '35 2 * * *'
    stake_token_contract = []
    deposit_pool_address = []

    pool_transaction_table_name = 'footprint_flow.origin_transactions_convex'
    token_config = {
        "csv_file_path": 'yield_aggregator/convex/convex_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address', 'Earn Token 2 Address', 'Earn Token 3 Address'],
        "pool_keys": ['Deposit Pool Address', 'Withdraw Contract Address'],
        "deposit_pool": ['Deposit Pool Address'],
        "stake_token_contract": ['Stake Token Contract Address']
    }

    def parse_config_csv(self):
        df = super(ConvexPool, self).parse_config_csv()
        self.stake_token_contract = self.dataframe_to_list(df, self.token_config['stake_token_contract'])
        self.deposit_pool_address = self.dataframe_to_list(df, self.token_config['deposit_pool'])

    def build_classify_function(self):
        """
        所有的分类 method 都是相对于 from_address 来描述的，
        用 from_address + token类型 + to_address 来定义一个转账的类型
        :return:
        """
        return """
        CASE
            WHEN op_user = from_address AND token_address {match_stake_tokens} AND to_address {to_match_contract} THEN 'deposit'
            WHEN op_user = to_address AND token_address {match_stake_tokens} AND from_address {from_match_contract} THEN 'withdraw'
            WHEN op_user = to_address AND token_address {match_profit_tokens} THEN 'profit'
            ELSE 'UnKnow'
        END AS operation,
        """.format(
            match_profit_tokens=SQLUtil.build_match_tokens(self.earn_tokens),
            match_stake_tokens=SQLUtil.build_match_tokens(self.stake_tokens),
            to_match_contract=SQLUtil.build_match_tokens(self.stake_token_contract),
            from_match_contract=SQLUtil.build_match_tokens(self.deposit_pool_address),
        )

if __name__ == '__main__':
    pool = ConvexPool()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)

    history_sql = pool.build_history_data_sql()
    print(history_sql)
    file1 = open('history_sql.sql', 'w')
    file1.write(history_sql)

    # print(pool.parse_history_data())

    pool.run_daily_job()

    # print(None or 'a')
