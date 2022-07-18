from common.common_dex_model import DexModel


class UniswapDex(DexModel):
    project_name = 'ethereum_dex_uniswap'
    task_name = 'uniswap_dex'
    # execution_time = '55 3 * * *'
    history_date = '2022-03-03'
    source_add_liquidity_sql_file = 'dex/ethereum/uniswap/uniswap_add_liquidity.sql'
    source_remove_liquidity_sql_file = 'dex/ethereum/uniswap/uniswap_remove_liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/uniswap/uniswap_swap.sql'


if __name__ == '__main__':
    pool = UniswapDex()
    # swap / add_liquidity / remove_liquidity
    # business = pool.get_business_type(pool.business_type['swap'])
    # business = pool.get_business_type(pool.business_type['add_liquidity'])
    # business1.create_all_data_view()
    business = pool.get_business_type(pool.business_type['remove_liquidity'])
    # business.parse_history_data()
    # business2.create_all_data_view()
    # daily_sql = business1.build_daily_data_sql()
    # daily_sql = business.build_history_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)
    # business.do_import_gsc_to_bigquery()
    # business.run_daily_job('2022-03-03')


    # 基础校验
    # business.validate()



