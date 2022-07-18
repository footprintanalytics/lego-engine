from utils.query_bigquery import query_bigquery
from validation.inner_validate.base_inner_validate import BaseInnerValidate


class SwapTokenISZeroValidate(BaseInnerValidate):
	validate_type = 'basic'
	validate_name = 'swap_token_is_zero'
	desc = 'swap 的 流水不应该 <= 0'
	slack_warn = False

	def get_self_data(self):
		token_amount = self.get_pool_data()
		return {
			'zero_address': token_amount,
		}

	def get_pool_data(self):
		sql = f"""select distinct exchange_contract_address as contract_address from {self.validate_table} where token_a_amount = 0 or token_b_amount = 0"""
		df = query_bigquery(sql)
		return df['contract_address'].to_list()

	def check_data(self, self_data: dict):
		zero_address = self_data['zero_address']
		if zero_address:
			return self.messsage(False, fail_message=f'池子{zero_address}有token_amount为0的池子', ex_data=self_data)
		return self.messsage(True, ex_data=self_data)


if __name__ == '__main__':
	"""执行demo"""
	# v = SwapTokenISZeroValidate({
	# 	'project': 'airswap',
	# 	'chain': 'ethereum',
	# 	'validate_table': 'footprint-etl.ethereum_dex_airswap.airswap_dex_swap',
	# }).validate()
	# print(v)
