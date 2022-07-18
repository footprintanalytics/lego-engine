from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil

class TrueFiPool(InvestPoolModel1):
    project_name = 'TrueFi'
    task_name = 'trueFi_pool_transactions'
    execution_time = '37 2 * * *'
    history_date = '2021-09-06'
    stake_token_contract = []
    deposit_pool_address = []

    pool_transaction_table_name = 'footprint_flow.origin_transactions_trueFi'
    token_config = {
        "csv_file_path": 'yield_aggregator/trueFi/trueFi_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address', 'Earn Token 2 Address', 'Earn Token 3 Address'],
        "pool_keys": ['Pool Address']
    }

if __name__ == '__main__':
    pool = TrueFiPool()

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
    # pool.parse_history_data()

    # print(None or 'a')
