import datetime

from utils.query_bigquery import query_bigquery
from validation.inner_validate.base_inner_validate import BaseInnerValidate

missing_data_fill_max_date = datetime.datetime(2040, 1, 1)


class AddLiquidityFirstTransactionValidate(BaseInnerValidate):
	validate_type = 'basic'
	validate_name = 'add_liquidity_first_transaction'
	desc = '增加流动性是池子的最早的一条流水'
	slack_warn = False

	def __init__(self, cfg):
		super().__init__(cfg)
		self.add_liquidity_table = cfg['add_liquidity_table']
		self.swap_table = cfg['swap_table']
		self.remove_liquidity_table = cfg['remove_liquidity_table']

	def get_self_data(self):
		add_liquidation_first_day = self.get_pool_first_block_time(self.add_liquidity_table)
		swap_first_day = self.get_pool_first_block_time(self.swap_table, 'exchange_contract_address')
		remove_liquidation_first_day = self.get_pool_first_block_time(self.remove_liquidity_table)
		return {
			'add_liquidation_first_day': add_liquidation_first_day,
			'swap_first_day': swap_first_day,
			'remove_liquidation_first_day': remove_liquidation_first_day,
		}

	def check_data(self, self_data: dict):
		add_liquidation_first_day = self_data['add_liquidation_first_day']
		swap_first_day = self_data['swap_first_day']
		remove_liquidation_first_day = self_data['remove_liquidation_first_day']
		ex_contract = (set(remove_liquidation_first_day.keys()) | set(swap_first_day.keys()))
		has_ex_contract = ex_contract - set(add_liquidation_first_day.keys())
		if has_ex_contract:
			return self.messsage(
				False,
				fail_message=f'有合约{has_ex_contract}缺失了增加流动性流水, 占比:{len(has_ex_contract) / len(ex_contract)}%',
				ex_data=self_data)
		total = []
		for key, value in add_liquidation_first_day.items():
			add_liquidation_first_day_is_min = value == min(
				value,
				swap_first_day.get(key, missing_data_fill_max_date),
				remove_liquidation_first_day.get(key, missing_data_fill_max_date)
			)
			if not add_liquidation_first_day_is_min:
				total.append(key)
		if total:
			return self.messsage(False, fail_message=f'合约{total} 的第一条流水不为增加流动性', ex_data=self_data)
		return self.messsage(True, ex_data=self_data)

	def get_pool_first_block_time(self, table_name, contract_column='exchange_address'):
		sql = f"""
SELECT 
	min(block_time) as date, 
	{contract_column} as contract 
FROM 
	`{table_name}` 
GROUP BY
	{contract_column}
"""
		df = query_bigquery(sql)
		df.set_index('contract', inplace=True)
		return df['date'].to_dict()


if __name__ == '__main__':
	"""执行demo"""
# v = AddLiquidityFirstTransactionValidate({
# 	'project': 'sushi',
# 	'chain': 'ethereum',
# 	'add_liquidity_table': 'footprint-etl.ethereum_dex_sushi.sushi_dex_add_liquidity_all',
# 	'swap_table': 'footprint-etl.ethereum_dex_sushi.sushi_dex_swap_all',
# 	'remove_liquidity_table': 'footprint-etl.ethereum_dex_sushi.sushi_dex_remove_liquidity_all'
# }).validate()
# print(v)
