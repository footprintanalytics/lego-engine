from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil

class AlchemixPool(InvestPoolModel1):
    project_name = 'Alchemix'
    task_name = 'alchemix_pool_transactions'
    execution_time = '33 2 * * *'
    history_date = '2021-09-01'
    stake_token_contract = []
    deposit_pool_address = []

    pool_transaction_table_name = 'footprint_flow.origin_transactions_alchemix'
    token_config = {
        "csv_file_path": 'yield_aggregator/alchemix/alchemix_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address', 'Earn Token 2 Address', 'Earn Token 3 Address'],
        "pool_keys": ['Pool Address']
    }

if __name__ == '__main__':
    pool = AlchemixPool()

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

    # print(None or 'a')
