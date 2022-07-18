from common.common_avalanche_dex_model import AvalancheDexModel


class PangolinDex(AvalancheDexModel):
    project_name = 'avalanche_dex_pangolin'
    task_name = 'avalanche_pangolin_dex'
    execution_time = '55 2 * * *'
    history_date = '2022-01-04'
    source_add_liquidity_sql_file = 'dex/avalanche/pangolin/pangolin_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/avalanche/pangolin/pangolin_remove_liquidity.sql'
    source_swap_sql_file = 'dex/avalanche/pangolin/swap.sql'



if __name__ == '__main__':
    pool = PangolinDex()
    # swap
    business = pool.get_business_type(pool.business_type['swap'])
    # add_liquidity
    business1 = pool.get_business_type(pool.business_type['add_liquidity'])
    # remove_liquidity
    business2 = pool.get_business_type(pool.business_type['remove_liquidity'])

    business.run_daily_job(date_str='2022-01-05')
    business1.run_daily_job(date_str='2022-01-05')
    business2.run_daily_job(date_str='2022-01-05')

    # daily data
    # daily_sql = business.build_daily_data_sql()
    # history data
    # daily_sql = business.build_history_data_sql()

    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    # business.parse_history_data()
    # business.create_all_data_view()
    # business1.parse_history_data()
    # business1.create_all_data_view()
    # business2.parse_history_data()
    # business2.create_all_data_view()
    # business.parse_history_data()
