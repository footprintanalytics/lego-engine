from validation.external_validate.bitquery.bitquery_validate import BitqueryValidate
from utils.query_bigquery import query_bigquery
import pydash

class BitqueryDexPairCountValidate(BitqueryValidate):
    validate_name = 'bitquery_dex_pair_count_validate'
    desc = '交易对数量校验：(external_value-self_value)/self_value>difference_rate'
    validate_args = {'difference_rate': 0.1}
    validate_target = 'pairs_count'

    # 获取外部数据
    def get_external_data(self):
        pairs, filter_ = self.get_pairs_from_bitquery()
        sell_pairs = list(set(list(map(lambda n:pydash.get(n,'baseCurrency.address')+pydash.get(n,'quoteCurrency.address'), list(filter(lambda n: n.get('side') == 'SELL', pairs))))))
        print(sell_pairs)
        return {self.validate_target: len(sell_pairs), 'func': filter_}

    # 获取内部对比数据
    def get_self_data(self):
        pairs_count_sql = """
        select count(distinct concat(token_a_address,token_b_address)) as {key} from {trades_table} where Date(block_time) between '{since}' and '{till}' and protocol_id = {protocol_id}
        """.format(key=self.validate_target, trades_table=self.validate_table[0], since=self.get_since_date(),till=self.validate_date, protocol_id=self.protocol_id)
        try:
            df = query_bigquery(pairs_count_sql, self.project_id)
            count = int(df.iloc[0, 0])
        except Exception as e:
            print(e)
            count = 0
        return {self.validate_target: count, 'func': pairs_count_sql}


if __name__ == '__main__':
    BitqueryDexPairCountValidate(
        cfg={'validate_date': '2021-11-10', 'validate_table': ['footprint-etl.ethereum_dex_sushi.sushi_dex_swap'],
             'chain': 'ethereum', 'project': 'sushi', 'protocol_id': 16}).validate()
