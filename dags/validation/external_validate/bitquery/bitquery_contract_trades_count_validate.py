from validation.external_validate.bitquery.bitquery_validate import BitqueryValidate
from utils.query_bigquery import query_bigquery
from utils.bigquery_utils import show_dataset_list
import pydash
from gql import Client, gql
from datetime import datetime, timedelta


class BitqueryContractTradesCountValidate(BitqueryValidate):
    validate_name = 'bitquery_contract_trades_count_validate'
    desc = '拿近30天平台前十个地址的合约交易数量校验：max(external_value,self_value)/min(external_value,self_value) > percentage'
    validate_args = {'percentage': 2}
    validate_target = 'trades_count'

    def get_contract_address_field(self, table):
        if 'add_liquidity' in table or 'remove_liquidity' in table:
            contract_address_field = 'exchange_address'
        elif 'trades' in table:
            contract_address_field = 'exchange_contract_address'
        else:
            contract_address_field = 'contract_address'
        return contract_address_field

    def get_address_top_10_sql(self):
        sql_list = list(
            map(
                lambda n:
                "select {transaction_hash_field} as tx_hash, {contract_address_field} as address,sum(1) as {key} from {table} where protocol_id = {protocol_id} and Date({time_field}) between '{since}' and '{till}' group by {contract_address_field},{transaction_hash_field} "
                    .format(transaction_hash_field='tx_hash',
                            contract_address_field=self.get_contract_address_field(n), key=self.validate_target,
                            table=n,
                            protocol_id=self.protocol_id,
                            time_field='block_time',
                            since=self.get_since_date(),
                            till=self.validate_date),
                self.validate_table
            )
        )
        sql = """
        select * from (
            select 
            *,
            row_number() over(order by {key} desc) as row_index
            from (
            select address,sum({key}) as {key} from ({table}) group by address
            )
        )
        where row_index <=10
        """.format(key=self.validate_target, table='\n union all \n'.join(sql_list))
        print('sql===>', sql)
        return sql

    def get_address_list(self):

        sql = self.get_address_top_10_sql()
        df = query_bigquery(sql, self.project_id)
        data_dict = df.to_dict(orient='records')
        return list(map(lambda n: n.get('address'), data_dict))

    def get_since_date(self):
        since = (datetime.strptime(self.validate_date, '%Y-%m-%d') + timedelta(days=-30)).strftime('%Y-%m-%d')
        return since

    def get_pairs_from_bitquery(self):
        print('start fetch bitquery data ')
        filter_ = """{
              ethereum(network: %s) {
                smartContractCalls(
                      date: {since: "%s", till: "%s"}
                      smartContractAddress: {in: %s}
                      external: true
                    ) {
                      count
                      contractAddress: smartContract {
                        address {
                          address
                        }
                      }
                      senders: count(uniq: senders)
                    }
                }
            }
            """ % (
        self.chain, self.get_since_date(), self.validate_date, str(self.get_address_list()).replace("'", '"'))
        # Execute the query on the transport
        print('query from bitquery ', filter_)
        result = self.gql_client.execute(gql(filter_))
        contract_calls = result['ethereum']['smartContractCalls']
        print('get contract_calls from bitquery nums ', len(contract_calls))
        # time.sleep(20)
        return contract_calls, filter_

    def get_external_data(self):
        contract_calls, filter_ = self.get_pairs_from_bitquery()
        trades_count = sum(list(map(lambda n: pydash.get(n, 'count'), contract_calls)))
        return {self.validate_target: trades_count, 'func': filter_}

        # 获取内部对比数据

    def get_self_data(self):
        sql = """
        select sum({key}) as {key} from ({table})
        """.format(key=self.validate_target, table=self.get_address_top_10_sql())
        df = query_bigquery(sql, self.project_id)
        amount = int(df.iloc[0, 0])
        return {self.validate_target: amount, 'func': sql}

    # 对比内外部数据
    def check_data(self, external_data: dict, self_data: dict):
        external_value, self_value = external_data.get(self.validate_target), self_data.get(self.validate_target)
        result = {
            'result_code': 0,
            'validate_data': {
                'validate_target': self.validate_target,
                'external_value': external_data.get(self.validate_target),
                'external_func': external_data.get('func'),
                'self_value': self_data.get(self.validate_target),
                'self_func': self_data.get('func')
            },
            'result_message': ''
        }

        if max(external_value,self_value)/min(external_value,self_value) > self.validate_args.get('percentage'):
            result['result_code'] = 1
        return result


if __name__ == '__main__':
    BitqueryContractTradesCountValidate(
        cfg={'validate_table': ['footprint-etl.ethereum_lending_aave.aave_lending_borrow'],
             'chain': 'ethereum', 'project': 'aave', 'protocol_id': 6}).validate()
