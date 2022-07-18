from common.common_dex_model import DexModel


class SynthetixDex(DexModel):
    project_name = 'ethereum_dex_synthetix'
    task_name = 'synthetix_dex'
    task_liquidity_name = 'synthetix_dex_liquidity'
    task_swap_name = 'synthetix_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/synthetix/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/synthetix/swap.sql'


if __name__ == '__main__':
    pool = SynthetixDex()

    business = pool.get_business_type(pool.business_type['swap'])

    # daily_sql = business.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    business.run_daily_job()
    business.parse_history_data()
    business.create_all_data_view()
