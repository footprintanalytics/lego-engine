from common.common_pool_model3 import InvestPoolModel3


class CompoundPool(InvestPoolModel3):
    project_name = 'Compound'
    task_name = 'compound_pool_transactions'
    execution_time = '40 2 * * *'
    source_event_sql_file = 'yield_aggregator/compound/compound_invest.sql'


if __name__ == '__main__':
    pool = CompoundPool()

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

    # pool.run_daily_job(date_str='2021-08-22')
    # pool.parse_history_data()
    # print(None or 'a')
