from common.common_pool_model3 import InvestPoolModel3


class AavePool(InvestPoolModel3):
    project_name = 'Aave'
    task_name = 'aave_pool_transactions'
    execution_time = '5 3 * * *'
    source_event_sql_file = 'yield_aggregator/aave/aave_invest.sql'


if __name__ == '__main__':
    pool = AavePool()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)

    history_sql = pool.build_history_data_sql()
    print(history_sql)
    file1 = open('history_sql.sql', 'w')
    file1.write(history_sql)
    #
    # print(pool.get_history_table_name())

    # pool.parse_history_data()
    pool.run_daily_job()
    # pool.create_all_data_view()
    # print(None or 'a')
