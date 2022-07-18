'''
@Project ：defi-up 
@File    ：pool_daily_stats_view_builder.py
@Author  ：Nick
@Date    ：2021/8/24 3:11 下午 
'''
from utils.bigquery_utils import create_or_update_view


class DailyStatsViewBuilder:
    @staticmethod
    def build_pool_daily_stats_view(transactions_table: str):
        view_sql = DailyStatsViewBuilder.build_pool_daily_stats_sql(transactions_table)
        view_name = transactions_table + '_daily_stats'
        print('build_pool_daily_stats_view ', view_sql)
        # create or update view
        create_or_update_view(view_name, view_sql)

    @staticmethod
    def build_pool_daily_stats_sql(transactions_table: str):
        return """
        SELECT 
            Date(block_timestamp) AS day,
            contract_address,
            token_address,
            MAX(token_symbol),
            SUM(
                CASE
                    WHEN operation = 'deposit' THEN value
                    ELSE 0
                END
            ) AS deposit,
            SUM(
                CASE
                    WHEN operation = 'withdraw' THEN value
                    ELSE 0
                END
            ) AS withdraw,
            SUM(
                CASE
                    WHEN operation = 'profit' THEN value
                    ELSE 0
                END
            ) AS profit,
        FROM `{transactions_table}`
        GROUP BY 1, 2, 3
        ORDER BY 1
        """.format(transactions_table=transactions_table)
