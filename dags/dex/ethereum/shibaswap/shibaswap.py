from common.common_dex_model import DexModel


class ShibaswapDex(DexModel):
    project_name = 'ethereum_dex_shibaswap'
    task_name = 'shibaswap_dex'
    task_liquidity_name = 'shibaswap_dex_liquidity'
    task_swap_name = 'shibaswap_dex_swap'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/shibaswap/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/shibaswap/shibaswap_swap.sql'



if __name__ == '__main__':
    pool = ShibaswapDex()

    # 同项目目录生成完整sql
    # daily_sql = pool.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql["swap_sql"])

    # 跑一天的数据
    # pool.run_daily_job()

    # 跑历史数据
    pool.parse_history_data()
