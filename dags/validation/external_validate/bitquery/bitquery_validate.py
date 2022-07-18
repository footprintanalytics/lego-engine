from validation.external_validate.base_external_validate import BaseExternalValidate
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from utils.query_bigquery import query_bigquery
from datetime import datetime,timedelta
import pydash
import time

exchange_name_mapping = {
    '1': {'exchange_name': 'Uniswap', 'protocol_name': 'Uniswap v2'},
    '403': {'exchange_name': 'AirSwap', 'protocol_name': 'AirSwap v2'},
    '12': {'exchange_name': 'Balancer', 'protocol_name': 'Balancer Pool Token'},
    '64': {'exchange_name': 'Bancor Network', 'protocol_name': 'Bancor Network v2'},
    '3': {'exchange_name': 'Curve', 'protocol_name': 'Curve'},
    '46': {'exchange_name': 'dYdX', 'protocol_name': 'dYdX2'},
    '450': {'exchange_name': 'Mooniswap', 'protocol_name': 'Mooniswap'},
    '451': {'exchange_name': 'Oasis', 'protocol_name': 'Matching Market'},
    '139': {'exchange_name': 'IDEX', 'protocol_name': 'IDEX'},
    '28': {'exchange_name': 'Kyber Network'},
    '48': {'exchange_name': 'Dodo', 'protocol_name': 'Dodo'},
    '16': {'exchange_name': 'SushiSwap', 'protocol_name': 'Uniswap v2'},
    '400': {'exchange_name': 'Uniswap', 'protocol_name': 'Uniswap'},
    '217': {'exchange_name': 'Uniswap', 'protocol_name': 'Uniswap v3'},
    '303': {'exchange_name': 'QuickSwap', 'protocol_name': 'Uniswap v2'},
    '291': {'exchange_name': 'SushiSwap', 'protocol_name': 'Uniswap v2'},
    '100': {'exchange_name': 'Pancake v2'},
}

chain_mapping={
    'ethereum':'ethereum',
    'binance':'bsc',
    'polygon':'matic'
}


class BitqueryValidate(BaseExternalValidate):
    transport = AIOHTTPTransport(url="https://graphql.bitquery.io",
                                 headers={'X-API-KEY': 'key'})
    gql_client = Client(transport=transport, fetch_schema_from_transport=True, execute_timeout=240)

    def __init__(self, cfg):
        super().__init__(cfg)
        if pydash.get(cfg, 'validate_args'):
            self.validate_args = pydash.get(cfg, 'validate_args')
        if pydash.get(cfg, 'validate_date'):
            self.validate_date = pydash.get(cfg, 'validate_date')

    def get_since_date(self):
        since = (datetime.strptime(self.validate_date, '%Y-%m-%d') + timedelta(days=-0)).strftime('%Y-%m-%d')
        return since

    def get_bitquery_name_and_protocol(self):
        name = ''
        protocol = None
        if not exchange_name_mapping.get(self.protocol_id):
            name = self.project
            protocol = ''
        elif pydash.get(exchange_name_mapping, '{}.protocol_name'.format(self.protocol_id)):
            name = pydash.get(exchange_name_mapping, '{}.exchange_name'.format(self.protocol_id))
            protocol = 'protocol(protocol:{{is: "{protocol}" }})'.format(
                protocol=pydash.get(exchange_name_mapping, '{}.protocol_name'.format(self.protocol_id)))
        else:
            name = pydash.get(exchange_name_mapping, '{}.exchange_name'.format(self.protocol_id))
            protocol = ''
        return name, protocol

    def get_pairs_from_bitquery(self):
        print('start fetch bitquery data ')

        name, protocol = self.get_bitquery_name_and_protocol()
        filter_ = """{
              ethereum(network: %s) {
                dexTrades(
                  options: {limit: 10000000, asc: "timeInterval.day"}
                  date: {since: "%s",till: "%s"}
                  exchangeName: {is: "%s"}
                ) {
                  timeInterval {
                    day(count: 1)
                  }
                  exchange{
                    name
                  }
                  baseCurrency {
                    symbol
                    address
                  }
                  quoteCurrency {
                    symbol
                    address
                  }
                  baseAmount
                  quoteAmount
                  tradeAmount(in: USD)
                  trades: count
                  side
                  %s
                }
              }
            }
            """ % (chain_mapping[self.chain],self.get_since_date(), self.validate_date, name, protocol)
        # Execute the query on the transport
        print('query from bitquery ', filter_)
        try:
            result = self.gql_client.execute(gql(filter_))
            pairs = result['ethereum']['dexTrades']
            print('get pairs from bitquery nums ', len(pairs))
        except Exception as e:
            print(e)
            pairs = []
        time.sleep(10)
        return pairs, filter_

    # 对比内外部数据
    def check_data(self, external_data: dict, self_data: dict):
        external_value, self_value = external_data.get(self.validate_target), self_data.get(self.validate_target)
        result = {
            'result_code': 0,
            'validate_data': {
                'validate_date': self.validate_date,
                'validate_target': self.validate_target,
                'external_value': external_data.get(self.validate_target),
                'external_func': external_data.get('func'),
                'self_value': self_data.get(self.validate_target),
                'self_func': self_data.get('func')
            },
            'result_message': ''
        }
        if external_value ==0 and self_value == 0:
            result['result_code']=0
        elif external_value ==0 or self_value ==0:
            result['result_code'] = 1
        elif abs(external_value - self_value) / self_value > self.validate_args.get('difference_rate'):
            result['result_code'] = 1

        return result
