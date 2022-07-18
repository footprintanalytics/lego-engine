from common.common_bsc_dex_model import DexBSCModel


class BiswapDex(DexBSCModel):
    project_name = 'bsc_dex_biswap'
    task_name = 'biswap_dex'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-19'
    source_swap_sql_file = 'dex/bsc/biswap/biswap_swap.sql'
    source_add_liquidity_sql_file = 'dex/bsc/biswap/biswap_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/bsc/biswap/biswap_remove_liquidity.sql'


if __name__ == '__main__':
    pool = BiswapDex()

    business = pool.get_business_type(pool.business_type['swap'])
    # business = pool.get_business_type(pool.business_type['add_liquidity'])
    # business = pool.get_business_type(pool.business_type['remove_liquidity'])

    business.run_daily_job()
    # business.parse_history_data()
    # business.create_all_data_view()
    # business.validate()
