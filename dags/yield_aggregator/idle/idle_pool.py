from common.common_pool_model1 import InvestPoolModel1


class IdlePool(InvestPoolModel1):
    project_name = 'Idle'
    task_name = 'idle_pool_transactions'
    execution_time = '30 2 * * *'

    pool_transaction_table_name = 'footprint_flow.origin_transactions_idle'
    token_config = {
        "csv_file_path": 'yield_aggregator/idle/idle_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address', 'Earn Token 2 Address', 'Earn Token 3 Address'],
        "pool_keys": ['Pool Address']
    }


if __name__ == '__main__':
    pool = IdlePool()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)
    #
    # history_sql = pool.build_history_data_sql()
    # print(history_sql)
    # file1 = open('history_sql.sql', 'w')
    # file1.write(history_sql)
    #
    # print(pool.get_history_table_name())

    # pool.run_daily_job(date_str='2021-08-22')
    # pool.create_all_data_view()

    # pool.create_pool_daily_view()
    # pool.parse_history_data()
    # print(None or 'a')
