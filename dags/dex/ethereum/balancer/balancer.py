from common.common_dex_model import DexModel


class BalancerDex(DexModel):
    project_name = 'ethereum_dex_balancer'
    task_name = 'balancer_dex'
    # execution_time = '55 3 * * *'
    source_add_liquidity_sql_file = 'dex/ethereum/balancer/balancer_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/ethereum/balancer/balancer_remove_liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/balancer/balancer_swap.sql'
    history_date = '2021-12-01'


if __name__ == '__main__':
    pool = BalancerDex()
    business = pool.get_business_type(pool.business_type['swap'])
    addLiquidityBusiness = pool.get_business_type(pool.business_type['add_liquidity'])
    removeLiquidityBusiness = pool.get_business_type(pool.business_type['remove_liquidity'])

    # daily_sql = business.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)

    # # business.run_daily_job()
    # addLiquidityBusiness.run_daily_job()
    # removeLiquidityBusiness.run_daily_job()
    #
    # # business.parse_history_data()
    # addLiquidityBusiness.parse_history_data()
    # removeLiquidityBusiness.parse_history_data()
    #
    # # business.create_all_data_view()
    # addLiquidityBusiness.create_all_data_view()
    # removeLiquidityBusiness.create_all_data_view()

    # business.validate()
    addLiquidityBusiness.validate()
    removeLiquidityBusiness.validate()

