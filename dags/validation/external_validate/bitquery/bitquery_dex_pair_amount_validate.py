from validation.external_validate.bitquery.bitquery_validate import BitqueryValidate
from utils.query_bigquery import query_bigquery
import pydash
import pandas as pd


class BitqueryDexPairAmountValidate(BitqueryValidate):
    validate_name = 'bitquery_dex_pair_amount_validate'
    desc = '取交易数前10的交易对的value进行校验: (external_value-self_value)/self_value>difference_rate'
    validate_args = {'difference_rate': 0.1}
    validate_target = 'trade_amount'

    # 获取外部数据
    def get_external_data(self):
        pairs, filter_ = self.get_pairs_from_bitquery()
        sell_pairs = list(filter(lambda n: n.get('side') == 'SELL', pairs))
        new_sell_pairs = list(map(lambda n: {
            'address': pydash.get(n, 'baseCurrency.address') + pydash.get(n, 'quoteCurrency.address'),
            'trades': pydash.get(n, 'trades'),
            'amount': pydash.get(n, 'baseAmount') + pydash.get(n, 'quoteAmount')
        }, sell_pairs))
        if len(new_sell_pairs) == 0:
            amount = 0
        else:
            df = pd.DataFrame(data=new_sell_pairs).groupby(by=['address']).sum().sort_values(by='trades',ascending=False)
            amount_list = list(df['amount'].values)[:9]
            amount = sum(amount_list)
        # set_sell_pairs = list(set(list(map(lambda n:pydash.get(n,'baseCurrency.address')+pydash.get(n,'quoteCurrency.address'),sell_pairs))))
        # set_sell_pairs_list =[]
        # for pair in set_sell_pairs:
        #     match_pair_list = list(filter(lambda n:pydash.get(n,'baseCurrency.address')+pydash.get(n,'quoteCurrency.address')==pair ,sell_pairs))
        #     set_sell_pairs_list.append({
        #         'trades':sum(list(map(lambda n:pydash.get(n, 'trades'),match_pair_list))),
        #         'baseAmount': sum(list(map(lambda n:pydash.get(n, 'baseAmount'),match_pair_list))),
        #         'quoteAmount': sum(list(map(lambda n:pydash.get(n, 'quoteAmount'),match_pair_list))),
        #     })
        #
        # set_sell_pairs_list.sort(key=lambda n: pydash.get(n, 'trades'), reverse=True)
        # pairs_top_10 = set_sell_pairs_list[:9]
        # amount = sum(list(map(lambda n: pydash.get(n, 'baseAmount') + pydash.get(n, 'quoteAmount'), pairs_top_10)))
        return {self.validate_target: amount, 'func': filter_}

    # 获取内部对比数据
    def get_self_data(self):
        pairs_count_sql = """
                with total as (
                select token_a_address,token_b_address,protocol_id,project,sum(token_a_amount) as a_amount,sum(token_b_amount) as b_amount,count(*) as trade_count 
                from {trades_table} 
                where Date(block_time) between '{since}' and '{till}'
                and protocol_id ={protocol_id}
                group by 1,2,3,4
                )
                
                select 
                sum(a_amount + b_amount) as {key}
                from (
                    select 
                    *,
                    row_number() over(partition by protocol_id order by trade_count desc) as row_index
                    from total
                ) where row_index <=10
            """.format(key=self.validate_target, trades_table=self.validate_table[0],since=self.get_since_date(), till=self.validate_date,
                       protocol_id=self.protocol_id)
        try:
            df = query_bigquery(pairs_count_sql, self.project_id)
            amount = int(df.iloc[0, 0])
        except Exception as e:
            print(e)
            amount = 0
        return {self.validate_target: amount, 'func': pairs_count_sql}


if __name__ == '__main__':
    BitqueryDexPairAmountValidate(
        cfg={'validate_date': '2021-11-10', 'validate_table': ['footprint-etl.ethereum_dex_sushi.sushi_dex_swap'],
             'chain': 'ethereum', 'project': 'sushi', 'protocol_id': 16}).validate()
