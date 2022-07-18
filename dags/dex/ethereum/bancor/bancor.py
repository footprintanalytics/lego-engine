from common.common_dex_model import DexModel


class bancorDex(DexModel):
    project_name = 'ethereum_dex_bancor'
    task_name = 'bancor_dex'
    task_liquidity_name = 'bancor_dex_liquidity'
    task_swap_name = 'bancor_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    source_add_liquidity_sql_file = 'dex/ethereum/bancor/bancor_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/ethereum/bancor/bancor_remove_liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/bancor/swap.sql'


if __name__ == '__main__':
    pool = bancorDex()
    business = pool.get_business_type(pool.business_type['swap'])
    businessAdd = pool.get_business_type(pool.business_type['add_liquidity'])
    businessRemoved = pool.get_business_type(pool.business_type['remove_liquidity'])

    # business.run_daily_job()
    # businessAdd.run_daily_job()
    # businessRemoved.run_daily_job()

    # business.parse_history_data()
    # businessAdd.parse_history_data()
    # businessRemoved.parse_history_data()


    # business.create_all_data_view()
    # businessAdd.create_all_data_view()
    # businessRemoved.create_all_data_view()

    # business.validate()
    businessAdd.validate()
    businessRemoved.validate()

    # daily_sql = pool.build_daily_data_sql()
    # print(daily_sql["add_liquidity_sql"])
    # print(daily_sql["swap_sql"])
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql["add_liquidity_sql"])
    # file1.write(daily_sql["swap_sql"])

    # history_sql = pool.build_history_data_sql()
    # print(history_sql["add_liquidity_sql"])
    # print(history_sql["swap_sql"])
    # file1 = open('history_sql.sql', 'w')
    # file1.write(history_sql["add_liquidity_sql"])
    # file1.write(history_sql["swap_sql"])
    #
    # print(pool.get_history_table_name())

    # pool.run_daily_job()
    # pool.parse_daily_swap_data()

    # pool.parse_history_data()
    # pool.create_all_data_view()
    # print(None or 'a')
