from common.common_polygon_dex_model import PolygonDexModel


class SushiDex(PolygonDexModel):
    project_name = 'polygon_dex_sushi'
    task_name = 'sushi_dex'
    task_liquidity_name = 'sushi_dex_liquidity'
    task_swap_name = 'sushi_dex_swap'
    execution_time = '5 3 * * *'
    history_date = '2021-12-07'
    source_swap_sql_file = 'dex/polygon/sushi/sushi_swap.sql'


if __name__ == '__main__':
    # pool = SushiDex()
    pool = SushiDex()


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
    # file1 = open('daily_swap_sql.sql', 'w')
    # file1.write(daily_sql)
    # file1.close()
    business.validate()


    # business.run_daily_job()
    # business.parse_daily_swap_data()

    # business.create_all_data_view()

