from common.common_dex_model import DexModel


class kyberDex(DexModel):
    project_name = 'ethereum_dex_kyber'
    task_name = 'kyber_dex'
    # execution_time = '55 3 * * *'
    history_date = '2021-12-01'
    # source_liquidity_sql_file = 'dex/ethereum/kyber/liquidity.sql'
    source_swap_sql_file = 'dex/ethereum/kyber/swap.sql'


if __name__ == '__main__':
    pool = kyberDex()

    business = pool.get_business_type(pool.business_type['swap'])

    # daily_sql = business.build_daily_data_sql()
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql) 3149425 + 8698 2066 + 1731045  3173191 + 2952

    # business.run_daily_job('2021-11-08')
    business.parse_history_data()
