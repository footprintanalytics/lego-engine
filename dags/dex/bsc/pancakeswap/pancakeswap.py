from common.common_bsc_dex_model import DexBSCModel


class PancakeSwapDex(DexBSCModel):
    project_name = 'bsc_dex_pancakeswap'
    task_name = 'pancakeswap_dex'
    skip_liquidity = False
    # execution_time = '45 15 * * *'  # BSC 的底层数据在 UTC 12 点之后才 load 完
    # history_date = '2021-10-24' # swap history_date
    history_date = '2021-12-19'
    start_date = '2021-04-22'  # BSC 4月份才开始爆发，大部分项目在这个时间之后
    source_swap_sql_file = 'dex/bsc/pancakeswap/swap.sql'
    source_add_liquidity_sql_file = 'dex/bsc/pancakeswap/pancakeswap_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/bsc/pancakeswap/pancakeswap_remove_liquidity.sql'


if __name__ == '__main__':
    pool = PancakeSwapDex()

    # daily_sql = pool.build_daily_data_sql()
    # # print(daily_sql["add_liquidity_sql"])
    # # print(daily_sql["swap_sql"])
    # file1 = open('daily_swap_sql.sql', 'w')
    # # file2 = open('daily_liquidity_sql.sql', 'w')
    # file1.write(daily_sql["swap_sql"])
    # # file2.write(daily_sql["add_liquidity_sql"])
    # file1.close()
    #
    # history_sql = pool.build_history_data_sql()
    # # print(history_sql["add_liquidity_sql"])
    # # print(history_sql["swap_sql"])
    # file1 = open('history_sql.sql', 'w')
    # # file1.write(history_sql["add_liquidity_sql"])
    # file1.write(history_sql["swap_sql"])
    # file1.close()
    # print(2)
    #
    # print(pool.get_history_table_name())

    # pool.run_daily_job()
    # pool.parse_daily_swap_data()

    # pool.parse_history_data()
    # pool.create_all_data_view()
    # print(None or 'a')

    business = pool.get_business_type(pool.business_type['swap'])

    daily_sql = business.build_daily_data_sql()
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)
    # business.validate()
    # business.run_daily_job()
    # business.parse_history_data()
    # business.create_all_data_view()

