from common.common_polygon_dex_model import PolygonDexModel



class QuickswapDex(PolygonDexModel):
    project_name = 'polygon_dex_quickswap'
    task_name = 'quickswap_dex'
    task_liquidity_name = 'quickswap_dex_liquidity'
    task_swap_name = 'quickswap_dex_swap'
    execution_time = '5 3 * * *'
    history_date = '2021-11-16'
    # source_liquidity_sql_file = 'dex/ethereum/quickswap/polygon_quickswap_liquidity.sql'
    source_swap_sql_file = 'dex/polygon/quickswap/quickswap_swap.sql'


if __name__ == '__main__':
    # pool = quickswapDex()
    pool = QuickswapDex()

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


    business = pool.get_business_type(pool.business_type['swap'])

    # daily_sql = business.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)
    business.validate()

    # business.run_daily_job('2021-11-03')
    # business.parse_history_data()
