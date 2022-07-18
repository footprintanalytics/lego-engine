from common.common_dex_model import DexModel


class ZeroexDex(DexModel):
    project_name = 'ethereum_dex_zeroex'
    task_name = 'zeroex_dex'
    task_liquidity_name = 'zeroex_dex_liquidity'
    task_swap_name = 'zeroex_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/zeroex/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/zeroex/swap.sql'


if __name__ == '__main__':
    pool = ZeroexDex()
    business = pool.get_business_type(pool.business_type['swap'])

    # 同项目目录生成完整sql
    # daily_sql = business.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql["swap_sql"])

    # 跑一天的数据
    # business.run_daily_job()

    # 跑历史数据
    # business.parse_history_data()
