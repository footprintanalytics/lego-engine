from utils.query_bigquery import query_bigquery
from validation.inner_validate.base_inner_validate import BaseInnerValidate

default_swap_rate_ratio_limit = 100


class SwapTokenSwapRateValidate(BaseInnerValidate):
	validate_type = 'basic'
	validate_name = 'swap_token_swap_rate'
	desc = 'swap 的 变化率的转化率变化夸张的池子'
	slack_warn = False

	def __init__(self, cfg):
		super().__init__(cfg)
		self.switch_rate_ratio_limit = cfg.get('switch_rate_ratio_limit', default_swap_rate_ratio_limit)

	def get_self_data(self):
		token_amount = self.get_pool_data()
		return {
			'rate_ratio': token_amount,
		}

	def get_pool_data(self):
		sql = f"""
WITH
  select_all AS (
  SELECT
    token_a_amount,
    token_b_amount,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    block_time
  FROM
    {self.validate_table}
  WHERE
    token_a_amount IS NOT NULL
    AND token_a_amount > 0
    AND token_b_amount IS NOT NULL
    AND token_b_amount > 0
    AND block_time BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
    ),
  rate_all AS (
  SELECT
    (token_a_amount / token_b_amount) AS rate,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    block_time
  FROM
    select_all
  UNION ALL
  SELECT
    (token_b_amount / token_a_amount) AS rate,
    token_a_address AS token_b_address,
    token_b_address AS token_a_address,
    exchange_contract_address,
    block_time
  FROM
    select_all )
SELECT
  MAX(rate)/AVG(rate) AS rate_ratio,
  SUM(1) AS count,
  token_a_address AS token_a_address,
  token_b_address AS token_b_address,
  exchange_contract_address AS exchange_contract_address
FROM
  rate_all
GROUP BY
  token_a_address,
  token_b_address,
  exchange_contract_address"""
		df = query_bigquery(sql)
		df = df.set_index(['token_a_address', 'token_b_address', 'exchange_contract_address'])
		return df['rate_ratio'].to_dict()

	def check_data(self, self_data: dict):
		rate_ratio = self_data['rate_ratio']
		over100rate = []
		for info, rate in rate_ratio.items():
			if rate > self.switch_rate_ratio_limit:
				over100rate.append([info, rate])
		if over100rate:
			return self.messsage(False, fail_message=f'池子{over100rate}的转换率变化率过于夸张的池子, 大于{self.switch_rate_ratio_limit}, 占比 {len(over100rate)}/{len(rate_ratio)}={round(len(over100rate)/len(rate_ratio)*100, 2)}%', ex_data=self_data)
		return self.messsage(True, ex_data=self_data)


if __name__ == '__main__':
	"""执行demo"""
	# v = SwapTokenSwapRateValidate({
	# 	'project': 'uniswap',
	# 	'chain': 'ethereum',
	# 	'validate_table': 'footprint-etl.ethereum_dex_uniswap.uniswap_dex_swap',
	# }).validate()
	# print(v)
