from common.common_dex_model import DexModel


class SushiDex(DexModel):
    project_name = 'ethereum_dex_sushi'
    task_name = 'sushi_dex'
    task_liquidity_name = 'sushi_dex_liquidity'
    task_swap_name = 'sushi_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    source_add_liquidity_sql_file = 'dex/ethereum/sushi/sushi_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/ethereum/sushi/sushi_remove_liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/sushi/sushi_swap.sql'




if __name__ == '__main__':
    pool = SushiDex()
    # pool = PolygonSushiDex()

    business = pool.get_business_type(pool.business_type['swap'])

    # daily_sql = business.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    business.run_daily_job('2021-11-15')
    # business.parse_history_data()
