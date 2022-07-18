from datetime import timedelta, date

from common.common_pool_model1 import InvestPoolModel1
from utils.sql_util import SQLUtil


class MakerDAOPool(InvestPoolModel1):
    project_name = 'MakerDAO'
    task_name = 'maker_dao_pool_transactions'
    execution_time = '5 3 * * *'
    history_date = '2021-09-13'

    token_config = {
        "csv_file_path": 'yield_aggregator/maker_dao/maker_dao_pools.csv',
        # "csv_file_path": 'yield_aggregator/maker_dao/maker_dao_dai_pools.csv',
        "stake_token_keys": ['token_address'],
        "earn_token_keys": [],
        "pool_keys": ['pool_address']
    }
    pool_transaction_table_name = None

    def build_origin_source_sql(self, match_date_filter: str):
        """
        从基础的交易表上把平台相关的交易过滤出来，查询量非常大，慎用
        :return:
        """
        return """
        SELECT
            tt.transaction_hash,
            tt.block_timestamp,
            t.from_address AS op_user,
            t.to_address AS contract_address,
            t.gas,
            t.gas_price,
            tt.from_address,
            tt.to_address,
            tt.token_address,
            CAST(tt.value AS FLOAT64 ) as value
        FROM
        `bigquery-public-data.crypto_ethereum.transactions` t
        LEFT JOIN `bigquery-public-data.crypto_ethereum.token_transfers` tt
        ON t.hash = tt.transaction_hash
        WHERE t.to_address {match_pool_address}
        AND Date(tt.block_timestamp) {match_date_filter}
        AND Date(t.block_timestamp) {match_date_filter}
        """.format(
            match_pool_address=SQLUtil.build_match_tokens(self.pool_address + ['0x0000000000000000000000000000000000000000']),
            match_date_filter=match_date_filter
        )

    def build_classify_function(self):
        """
        所有的分类 method 都是相对于 from_address 来描述的，
        用 from_address + token类型 + to_address 来定义一个转账的类型
        :return:
        """
        profit_match = ""
        if len(self.earn_tokens) > 0:
            profit_match = "WHEN op_user = to_address AND token_address {match_profit_tokens} THEN 'profit'" \
                .format(match_profit_tokens=SQLUtil.build_match_tokens(self.earn_tokens), )
        return """
        CASE
            WHEN op_user = from_address AND token_address {match_stake_tokens} AND to_address {match_contract} THEN 'deposit'
            WHEN op_user = to_address AND token_address {match_stake_tokens} AND from_address {match_contract} THEN 'withdraw'
            {profit_match}
            ELSE 'UnKnow'
        END AS operation,
        """.format(
            profit_match=profit_match,
            match_stake_tokens=SQLUtil.build_match_tokens(self.stake_tokens),
            match_contract=SQLUtil.build_match_tokens(self.pool_address + ['0x0000000000000000000000000000000000000000']),
        )


def daterange(start_date, end_date):
    for n in range(1, int((end_date - start_date).days) + 1):
        yield (end_date - timedelta(n)).strftime("%Y-%m-%d")


def etl_by_date():
    pool = MakerDAOPool()
    start_date = date(2021, 8, 1)
    # start_date = date(2020, 9, 11)
    end_date = date(2021, 9, 1)
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
    pool = MakerDAOPool()



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
    #
    # pool.run_daily_job(date_str='2021-09-01')
    # # pool.create_all_data_view()
    #
    # # pool.create_pool_daily_view()
    # pool.parse_history_data()
    # print(None or 'a')

    # with open('./curve_v1_pools.json', 'r') as f:
    #     data = json.load(f)
    #     df = pd.DataFrame(data, columns=['pool_address', 'pool_name', 'token_symbol', 'token_address',
    #                                      'decimals'])
    #     print(data)
    #     df.to_csv('./curve_v1_pools.csv', index=False)
