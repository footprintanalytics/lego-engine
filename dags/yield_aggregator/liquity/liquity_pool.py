from common.common_pool_model1 import InvestPoolModel1
from datetime import date, timedelta
import csv
import os
from config import project_config


class Liquity(InvestPoolModel1):
    project_name = 'Liquity'
    task_name = 'liquity_pool_transactions'
    execution_time = '30 2 * * *'

    # pool_transaction_table_name = 'footprint_flow.origin_transactions_liquity'
    token_config = {
        "csv_file_path": 'yield_aggregator/liquity/liquity_pools.csv',
        "stake_token_keys": ['Stake Token Address'],
        "earn_token_keys": ['Earn Token 1 Address'],
        "pool_keys": ['Pool Address'],
        "proxy_address": ['proxy_address']
    }

    def get_operation_definition(self):
        dags_folder = project_config.dags_folder
        with open(os.path.join(dags_folder, self.token_config['csv_file_path'])) as f:
            info = csv.DictReader(f)
            deposit_definition = []
            withdraw_definition = []
            profit_definition = []
            for item in info:
                poo_address, stake_token_address, earn_token_address = item.get('Pool Address'), item.get(
                    'Stake Token Address'), item.get('Earn Token 1 Address')
                proxy_address = item.get('proxy_address')
                if poo_address == '0x24179cd81c9e782a4096035f7ec97fb8b783e007':
                    deposit_definition.append(
                        " (op_user = from_address and token_address = '{stake_token_address}' and to_address='{poo_address}') ".format(
                            stake_token_address=stake_token_address, poo_address=poo_address))
                    withdraw_definition.append(
                        " (op_user = to_address and token_address = '{stake_token_address}' and from_address='{proxy_address}') ".format(
                            stake_token_address=stake_token_address, proxy_address=proxy_address))
                else:
                    deposit_definition.append(
                        " (op_user = from_address and token_address = '{stake_token_address}' and to_address='{poo_address}') ".format(
                            stake_token_address=stake_token_address, poo_address=poo_address))
                    withdraw_definition.append(
                        " (op_user = to_address and token_address = '{stake_token_address}' and from_address='{poo_address}') ".format(
                            stake_token_address=stake_token_address, poo_address=poo_address))
                    profit_definition.append(
                        " (op_user = to_address and token_address = '{earn_token_address}' and contract_address != '0x24179cd81c9e782a4096035f7ec97fb8b783e007') ".format(
                            earn_token_address=earn_token_address))
            return 'or'.join(deposit_definition), 'or'.join(withdraw_definition), 'or'.join(profit_definition)

    def build_classify_function(self):
        """
        所有的分类 method 都是相对于 from_address 来描述的，
        用 from_address + token类型 + to_address 来定义一个转账的类型
        :return:
        """
        deposit_definition, withdraw_definition, profit_definition = self.get_operation_definition()
        return '''
        CASE 
                WHEN {deposit_definition} THEN 'deposit' 
                WHEN {withdraw_definition} THEN 'withdraw' 
                WHEN {profit_definition} THEN 'profit'
                ELSE 'UnKnow' 
        END AS operation,
        '''.format(
            deposit_definition=deposit_definition,
            withdraw_definition=withdraw_definition,
            profit_definition=profit_definition
        )


def daterange(start_date, end_date):
    for n in range(1, int((end_date - start_date).days) + 1):
        yield (end_date - timedelta(n)).strftime("%Y-%m-%d")

def etl_by_date():
    pool = Liquity()
    start_date = date(2021, 8, 20)
    # start_date = date(2020, 9, 11)
    end_date = date(2021, 9, 26)
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
    pool = Liquity()

    daily_sql = pool.build_daily_data_sql()
    print(daily_sql)
    file1 = open('daily_sql.sql', 'w')
    file1.write(daily_sql)

    history_sql = pool.build_history_data_sql()
    print(history_sql)
    file1 = open('history_sql.sql', 'w')
    file1.write(history_sql)

    # print(pool.get_history_table_name())

    pool.run_daily_job(date_str='2021-09-26')
    # etl_by_date()
    # pool.create_all_data_view()

    # pool.create_pool_daily_view()
    # pool.parse_history_data()
    # print(None or 'a')
