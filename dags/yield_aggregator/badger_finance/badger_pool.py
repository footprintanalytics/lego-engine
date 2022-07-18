from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil

class BadgerPool(InvestPoolModel1):
    project_name = 'Badger'
    task_name = 'badger_pool_transactions'
    execution_time = '43 2 * * *'
    history_date = '2021-09-05'
    stake_token_contract = []
    deposit_pool_address = []

    pool_transaction_table_name = 'footprint_flow.origin_transactions_badger'
    token_config = {
        "csv_file_path": 'yield_aggregator/badger_finance/badger_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": [],
        "pool_keys": ['Pool Address']
    }

if __name__ == '__main__':
    pool = BadgerPool()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)

    history_sql = pool.build_history_data_sql()
    print(history_sql)
    file1 = open('history_sql.sql', 'w')
    file1.write(history_sql)

    # print(pool.parse_history_data())
    # pool.create_all_data_view()
    # pool.run_daily_job()

    # print(None or 'a')
