'''
@Project ：defi-up 
@File    ：pool_all_data_view_builder.py
@Author  ：Nick
@Date    ：2021/8/24 3:11 下午 
'''
from utils.bigquery_utils import create_or_update_view


class AllDataViewBuilder:

    @staticmethod
    def merge_data_table(table: str, history_table: str, view_name: str):
        view_sql = """
            SELECT
              *
            FROM
              `{table}`
            UNION ALL
            SELECT
              *
            FROM
              `{history_table}`
        """.format(table=table, history_table=history_table)
        print('merge_data_table: ', view_sql)
        create_or_update_view(view_name, view_sql)

    @staticmethod
    def build_all_data_view(transactions_table: str, transactions_history_table: str, date_column: str, history_date: str):
        view_sql = AllDataViewBuilder.build_all_data_sql(transactions_table, transactions_history_table, date_column, history_date)
        view_name = transactions_table + '_all'
        print('build_all_data_sql ', view_sql)
        # create or update view
        create_or_update_view(view_name, view_sql)

    @staticmethod
    def build_all_data_sql(transactions_table: str, transactions_history_table: str, date_column: str, history_date: str):
        return """
        SELECT * FROM `{transactions_table}` WHERE {date_column} >= '{history_date}'
        UNION ALL
        SELECT * FROM `{transactions_history_table}` WHERE {date_column} < '{history_date}'
        """.format(transactions_table=transactions_table, date_column=date_column, history_date=history_date,
                   transactions_history_table=transactions_history_table)

    @staticmethod
    def build_multiply_data_view(table_list: list, view_name: str, sql: str):
        view_sql = ''
        union = ' UNION ALL '
        for i in range(len(table_list)):
            if i == len(table_list) - 1:
                union = ''
            view_sql += """ SELECT * FROM `{table}` """.format(table=table_list[i])
            view_sql += union

        view_sql = sql.format(dex_source=view_sql)
        print(view_name, view_sql)
        create_or_update_view(view_name, view_sql)
