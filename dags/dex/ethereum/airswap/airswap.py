from common.common_dex_model import DexModel


class airswapDex(DexModel):
    project_name = 'ethereum_dex_airswap'
    task_name = 'airswap_dex'
    task_liquidity_name = 'airswap_dex_liquidity'
    task_swap_name = 'airswap_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    source_swap_sql_file = 'dex/ethereum/airswap/swap.sql'


if __name__ == '__main__':
    pool = airswapDex()
    business = pool.get_business_type(pool.business_type['swap'])

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

    business.run_daily_job()
    # pool.parse_daily_swap_data()

    # pool.parse_history_data()
    # pool.create_all_data_view()
    # print(None or 'a')
