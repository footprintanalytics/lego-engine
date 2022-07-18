from common.common_dex_model import DexModel


class swaprDex(DexModel):
    project_name = 'ethereum_dex_swapr'
    task_name = 'swapr_dex'
    task_liquidity_name = 'swapr_dex_liquidity'
    task_swap_name = 'swapr_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/swapr/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/swapr/swap.sql'


if __name__ == '__main__':
    pool = swaprDex()

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

    pool.run_daily_job()
    # pool.parse_daily_swap_data()

    pool.parse_history_data()
    # pool.create_all_data_view()
    # print(None or 'a')
