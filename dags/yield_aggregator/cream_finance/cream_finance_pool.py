from datetime import timedelta, date

from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil


class CreamFinancePool(InvestPoolModel1):
    project_name = 'Cream-Finance'
    task_name = 'cream_finance_pool_transactions'
    execution_time = '50 2 * * *'
    history_date = '2021-09-06'

    token_config = {
        "csv_file_path": 'yield_aggregator/cream_finance/cream_finance_pools.csv',
        "stake_token_keys": ['token_address'],
        "earn_token_keys": [],
        "pool_keys": ['pool_address']
    }
    pool_transaction_table_name = None


def daterange(start_date, end_date):
    for n in range(1, int((end_date - start_date).days) + 1):
        yield (end_date - timedelta(n)).strftime("%Y-%m-%d")


def etl_by_date():
    pool = CreamFinancePool()
    start_date = date(2021, 9, 6)
    # start_date = date(2020, 9, 11)
    end_date = date(2021, 9, 14)
    for item in daterange(start_date, end_date):
        # date_str = single_date.strftime("%Y-%m-%d")
        try:
            print(item)
            pool.run_daily_job(date_str=item)
            # etl_one_date(item)
        except BaseException as e:
            print(e)
            continue


if __name__ == '__main__':
    pool = CreamFinancePool()

    # daily_sql = pool.build_daily_data_sql()
    # print(daily_sql)
    # file1 = open('daily_sql.sql', 'w')
    # file1.write(daily_sql)
    #
    # history_sql = pool.build_history_data_sql()
    # print(history_sql)
    # file1 = open('history_sql.sql', 'w')
    # file1.write(history_sql)

    # etl_by_date()
    #
    # print(pool.get_history_table_name())

    # pool.run_daily_job(date_str='2021-09-13')
    pool.create_all_data_view()

    # pool.create_pool_daily_view()
    # pool.parse_history_data()
    # print(None or 'a')

    # with open('./curve_v1_pools.json', 'r') as f:
    #     data = json.load(f)
    #     df = pd.DataFrame(data, columns=['pool_address', 'pool_name', 'token_symbol', 'token_address',
    #                                      'decimals'])
    #     print(data)
    #     df.to_csv('./curve_v1_pools.csv', index=False)
