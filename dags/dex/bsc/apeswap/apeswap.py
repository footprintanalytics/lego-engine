from common.common_bsc_dex_model import DexBSCModel


class ApeswapDex(DexBSCModel):
    project_name = 'bsc_dex_apeswap'
    task_name = 'apeswap_dex'
    task_liquidity_name = 'apeswap_dex_liquidity'
    task_swap_name = 'apeswap_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-19'
    # source_liquidity_sql_file = 'dex/ethereum/apeswap/liquidity.sql'
    source_swap_sql_file = 'dex/bsc/apeswap/apeswap_swap.sql'



if __name__ == '__main__':
    pool = ApeswapDex()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql["swap_sql"])

    # 跑一天的数据
    # pool.run_daily_job()

    # 跑历史数据
    # pool.parse_history_data()
