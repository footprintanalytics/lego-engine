from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil
from datetime import timedelta, date

class RariCapitalPool(InvestPoolModel1):
    project_name = 'RariCapital'
    task_name = 'rariCapital_pool_transaction'
    execution_time = '30 2 * * *'
    history_date = '2021-09-10'

    token_config = {
        "csv_file_path": 'yield_aggregator/rari_capital/rari_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address'],
        "pool_keys": ['Pool Address'],
        "controller_keys": ['Pool Controller Address']
    }
    def parse_config_csv(self):
        df = super(RariCapitalPool, self).parse_config_csv()
        self.controller_keys = self.dataframe_to_list(df, self.token_config['controller_keys'])
        print('check config controller_keys', self.controller_keys)

    def build_classify_function(self):
        return """
        CASE
            WHEN op_user = from_address AND to_address {controller_keys} AND token_address {stake_token_kyes} THEN 'deposit'
            WHEN from_address {pool_name} AND to_address {controller_keys} AND token_address {stake_token_kyes} THEN 'deposit'
            WHEN op_user = to_address AND from_address {pool_name} OR from_address {controller_keys} AND token_address {stake_token_kyes} AND to_address = op_user THEN 'withdraw'
            ELSE 'UnKnow'
        END AS operation,
        """.format(
            controller_keys=SQLUtil.build_match_tokens(self.controller_keys),
            pool_name=SQLUtil.build_match_tokens(self.pool_address),
            stake_token_kyes=SQLUtil.build_match_tokens(self.stake_tokens)
        )


def daterange(start_date, end_date):
    for n in range(1, int((end_date - start_date).days) + 1):
        yield (end_date - timedelta(n)).strftime("%Y-%m-%d")

def etl_by_date():
    pool = RariCapitalPool()
    start_date = date(2021, 9, 18)
    end_date = date(2021, 9, 22)
    for item in daterange(start_date, end_date):
        # date_str = single_date.strftime("%Y-%m-%d")
        try:
            print(item)
            pool.run_daily_job(date_str=item)
            # etl_one_date(item)
        except BaseException as e:
            print(e)
            continue

if __name__ == '__main__':
    # rari = RariCapitalPool()
    # daily_sql = rari.build_daily_data_sql()
    # print(daily_sql)
    # with open('build_daily_data_sql.sql', 'w') as f:
    #     f.write(daily_sql)
    #     f.close()
    # history_sql = rari.build_history_data_sql()
    # print(history_sql)
    # with open('build_history_data_sql.sql', 'w') as f:
    #     f.write(history_sql)
    #     f.close()
    # print(rari.get_history_table_name())
    # print(rari.get_daily_table_name())
    etl_by_date()
    # rari.create_all_data_view()
    # rari.parse_history_data()
    # rari.run_daily_job(date_str='2021-09-15')