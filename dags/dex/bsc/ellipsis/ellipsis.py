from common.common_bsc_dex_model import DexBSCModel


class Ellipsis(DexBSCModel):
    project_name = 'bsc_dex_ellipsis'
    task_name = 'ellipsis_dex'
    task_liquidity_name = 'ellipsis_dex_liquidity'
    task_swap_name = 'ellipsis_dex_swap'
    # execution_time = '45 15 * * *'
    history_date = '2021-12-19'
    source_swap_sql_file = 'dex/bsc/ellipsis/swap.sql'
    source_add_liquidity_sql_file = 'dex/bsc/ellipsis/ellipsis_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/bsc/ellipsis/ellipsis_remove_liquidity.sql'


if __name__ == '__main__':
    pool = Ellipsis()

    business = pool.get_business_type(pool.business_type['swap'])
    addLiquidityBusiness = pool.get_business_type(pool.business_type['add_liquidity'])
    removeLiquidityBusiness = pool.get_business_type(pool.business_type['remove_liquidity'])

    daily_sql = business.build_daily_data_sql()
    file1 = open('daily_swap_sql.sql', 'w')
    file1.write(daily_sql)
    file1.close()

    # business.run_daily_job()
    # business.parse_history_data()
    # business.create_all_data_view()

    # addLiquidityBusiness.run_daily_job()
    # addLiquidityBusiness.parse_history_data()
    # addLiquidityBusiness.create_all_data_view()

    removeLiquidityBusiness.run_daily_job('2021-11-16')
    # removeLiquidityBusiness.parse_history_data()
    # removeLiquidityBusiness.create_all_data_view()

    # business.validate()
    # addLiquidityBusiness.validate()
    # removeLiquidityBusiness.validate()