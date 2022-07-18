from common.common_fantom_dex_model import FantomDexModel


class SpookySwapDex(FantomDexModel):
    project_name = 'fantom_dex_spookyswap'
    task_name = 'fantom_spookyswap_dex'
    execution_time = '5 3 * * *'
    history_date = '2022-02-17'
    source_swap_sql_file = 'dex/fantom/spookyswap/spookyswap_swap.sql'



if __name__ == '__main__':
    pool = SpookySwapDex()
    # swap
    business = pool.get_business_type(pool.business_type['swap'])
    # add_liquidity
    # business1 = pool.get_business_type(pool.business_type['add_liquidity'])
    # # remove_liquidity
    # business2 = pool.get_business_type(pool.business_type['remove_liquidity'])

    # business.run_daily_job(date_str='2022-02-17')
    # business1.run_daily_job(date_str='2022-01-05')
    # business2.run_daily_job(date_str='2022-01-05')

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
