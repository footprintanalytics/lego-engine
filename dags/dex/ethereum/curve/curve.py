from common.common_dex_model import DexModel


class Curve(DexModel):
    project_name = 'ethereum_dex_curve'
    task_name = 'curve_dex'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-05'
    source_swap_sql_file = 'dex/ethereum/curve/curve_swap.sql'
    source_add_liquidity_sql_file = 'dex/ethereum/curve/curve_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/ethereum/curve/curve_remove_liquidity.sql'


if __name__ == '__main__':
    pool = Curve()

    business = pool.get_business_type(pool.business_type['swap'])
    # business = pool.get_business_type(pool.business_type['add_liquidity'])
    # business = pool.get_business_type(pool.business_type['remove_liquidity'])

    business.validate()
    # business.run_daily_job()
    # business.parse_history_data()
    # business.create_all_data_view()
