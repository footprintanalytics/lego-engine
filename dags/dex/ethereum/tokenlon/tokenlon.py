from common.common_dex_model import DexModel


class TokenlonDex(DexModel):
    project_name = 'ethereum_dex_tokenlon'
    task_name = 'tokenlon_dex'
    task_liquidity_name = 'tokenlon_dex_liquidity'
    task_swap_name = 'tokenlon_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/tokenlon/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/tokenlon/swap.sql'


if __name__ == '__main__':
    pool = TokenlonDex()

    # swap / add_liquidity / remove_liquidity
    business = pool.get_business_type(pool.business_type['swap'])
    # business = pool.get_business_type(pool.business_type['add_liquidity'])
    # business = pool.get_business_type(pool.business_type['remove_liquidity'])

    # daily_sql = business.build_daily_data_sql()
    # daily_sql = business.build_history_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    business.run_daily_job('2021-11-16')
    business.parse_history_data()
