import pandas as pd

from utils.query_bigquery import query_bigquery
from validation.inner_validate.base_inner_validate import BaseInnerValidate

decimal_delta = -10  # 允许的误差


class TokenBlanceOverZeroValidate(BaseInnerValidate):
	validate_type = 'basic'
	validate_name = 'token_balance_over_zero'
	desc = '池子的币数, 不会少于0'
	slack_warn = False

	def __init__(self, cfg):
		super().__init__(cfg)
		self.add_liquidity_table = cfg['add_liquidity_table']
		self.swap_table = cfg['swap_table']
		self.remove_liquidity_table = cfg['remove_liquidity_table']

	def get_self_data(self):
		token_amount = self.get_pool_data(self.add_liquidity_table, self.swap_table, self.remove_liquidity_table)
		return {
			'token_amount': token_amount,
		}

	def check_data(self, self_data: dict):
		token_amount = self_data['token_amount']
		total = []
		for key, value in token_amount.items():
			if not pd.isna(value) and value < decimal_delta:
				total.append((key, value))
		if total:
			return self.messsage(
				False,
				fail_message=f'池子{total}, 不符合校验规则, 比例{len(total)}/{len(token_amount)}={int(len(total) / len(token_amount) * 100)}%',
				ex_data=self_data)
		return self.messsage(True, ex_data=self_data)

	def get_pool_data(
			self, add_liquidity_all, swap_all, remove_liquidity_all
	):
		sql = f"""
SELECT
  SUM(amount) AS amount,
  token AS token,
  contract AS contract
FROM (
  SELECT
    exchange_address AS contract,
    token_address AS token,
    token_amount AS amount
  FROM
    `{add_liquidity_all}`
  UNION ALL
  SELECT
    exchange_address AS contract,
    token_address AS token,
    (-1) * token_amount AS amount
  FROM
    `{remove_liquidity_all}`
  UNION ALL
  SELECT
    exchange_contract_address AS contract,
    token_a_address AS token,
    (-1)*token_a_amount AS amount
  FROM
    `{swap_all}`
  UNION ALL
  SELECT
    exchange_contract_address AS contract,
    token_b_address AS token,
    token_b_amount AS amount
  FROM
    `{swap_all}` )

GROUP BY
  contract,
  token
"""
		df = query_bigquery(sql)
		df.set_index(['contract', 'token'], inplace=True)
		return df['amount'].to_dict()


if __name__ == '__main__':
	"""执行demo"""
	# v = TokenBlanceOverZeroValidate({
	# 	'project': 'sushi',
	# 	'chain': 'ethereum',
	# 	'add_liquidity_table': 'footprint-etl.ethereum_dex_sushi.sushi_dex_add_liquidity_all',
	# 	'swap_table': 'footprint-etl.ethereum_dex_sushi.sushi_dex_swap_all',
	# 	'remove_liquidity_table': 'footprint-etl.ethereum_dex_sushi.sushi_dex_remove_liquidity_all'
	# }).validate()
	# print(v)
