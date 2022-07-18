import datetime

import web3
from google.cloud import bigquery
from web3 import Web3

from utils.constant import CHAIN, BUSINESS_TYPE, BUSINESS_SECOND_TYPE, PROJECT_PATH
from utils.csv_util import ensure_dir
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery_base, get_schema

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

erc_20_stable_table = 'xed-project-237404.footprint_etl.'

business_type_with_token_key = {
	BUSINESS_TYPE['DEX']: [
		{'second_type': 'trades', 'address_column': 'token_a_address'},
		{'second_type': 'trades', 'address_column': 'token_b_address'},
	],
	BUSINESS_TYPE['LENDING']: [
		{'second_type': BUSINESS_SECOND_TYPE['SUPPLY'], 'address_column': 'asset_address'},
		{'second_type': BUSINESS_SECOND_TYPE['REPAY'], 'address_column': 'asset_address'},
		{'second_type': BUSINESS_SECOND_TYPE['BORROW'], 'address_column': 'asset_address'},
		{'second_type': BUSINESS_SECOND_TYPE['WITHDRAW'], 'address_column': 'asset_address'},
	]
}

# name 获取请从 defi_protocol_info寻找 limit配置从数据量猜测, 默认100即可
# erc_20_table 看具体表的命名.
ChainConfig = {
	CHAIN['ETHEREUM']: {
		'name': 'Ethereum',
		'business_type': [BUSINESS_TYPE['DEX']],
		'transaction_lower_limit': 100,
		'erc_20_table': 'erc20_tokens',
		'node_address': 'https://eth-mainnet.alchemyapi.io/v2/XF_w-nJIEOkjy6f2Ea-RvGUozfB0egVy'
	},
	CHAIN['BSC']: {
		'name': 'Binance',
		'business_type': [BUSINESS_TYPE['DEX']],
		'transaction_lower_limit': 1000,
		'erc_20_table': 'bsc_erc20_tokens',
		'node_address': 'https://bsc-dataseed1.binance.org'
	},
	CHAIN['POLYGON']: {
		'name': 'Polygon',
		'business_type': [BUSINESS_TYPE['DEX']],
		'transaction_lower_limit': 100,
		'erc_20_table': 'polygon_erc20_tokens',
		'node_address': 'https://polygon-mainnet.g.alchemy.com/v2/_3pLl92ylO-dC4W60B3Z9v8mJc0yeKNS'
	},
	CHAIN['ARBITRUM']: {
		'name': 'Arbitrum',
		'business_type': [BUSINESS_TYPE['DEX']],
		'transaction_lower_limit': 50,
		'erc_20_table': 'arbitrum_erc20_tokens',
		'node_address': 'http://10.202.0.37:8547/rpc'
	},
	CHAIN['FANTOM']: {
		'name': 'Fantom',
		'business_type': [BUSINESS_TYPE['DEX']],
		'transaction_lower_limit': 100,
		'erc_20_table': 'fantom_erc20_tokens',
		'node_address': 'https://rpc.ankr.com/fantom'
	},
}


def _base_query(chain_name: str, business_type: str, business_second_type: str, address_column: str) -> str:
	return f"""
	select LOWER({address_column}) as address 
	from `footprint-etl.footprint_{business_type}.{business_type}_{business_second_type}`t
	left join `xed-project-237404.footprint_etl.defi_protocol_info` d
	on t.protocol_id = d.protocol_id where d.chain = '{chain_name}'
"""


def _get_token(chain: str, conf: dict) -> [str]:
	chain_name = conf['name']
	transaction_lower_limit = conf.get('transaction_lower_limit', 100)
	business_types = conf['business_type']
	erc_20_table = conf['erc_20_table']

	platform_queries = []
	for _business_type in business_types:
		for second_types_info in business_type_with_token_key[_business_type]:
			platform_queries.append(_base_query(
				chain_name,
				_business_type,
				second_types_info['second_type'],
				second_types_info['address_column']
			))
	dex_query = 'union all'.join(platform_queries)
	query = f"""
		with dex_data as ({dex_query}),
		normal_token as (
			select address as address, sum(1) as c from dex_data
			where char_length(address) =42 and left(address, 2) = '0x'
			group by address having c > {transaction_lower_limit}
		),
		missing_or_exists as (
			select contract_address as address from `xed-project-237404.footprint_etl.erc20_token_error` where chain='{chain}'
			union all  
			select contract_address as address from `{erc_20_stable_table}{erc_20_table}`
		)
select normal_token.address as address from normal_token left join missing_or_exists on missing_or_exists.address=normal_token.address where missing_or_exists.address is null"""
	bqclient = bigquery.Client(project='footprint-etl-internal')
	df = bqclient.query(query).result().to_dataframe()
	return list(df['address'])


def create_get_token_info_task(dag: models.DAG, chain: str, conf: dict) -> PythonOperator:
	def get_token_info(**kwargs):
		tokens = _get_token(chain, conf)
		save_path = PROJECT_PATH + f'/../data/erc20_token/{chain}/'
		error_path = PROJECT_PATH + f'/../data/erc20_token/error/'
		save_name = f'{chain}=={datetime.datetime.now()}.csv'
		ensure_dir(save_path)
		ensure_dir(error_path)
		abi = """[
		{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"}]
		"""
		w3 = web3.main.Web3(Web3.HTTPProvider(
			conf['node_address'],
			request_kwargs={'timeout': 30}
		))
		with open(save_path + save_name, 'w+') as save, open(error_path + save_name, 'w+') as error:
			# 写入csv的第一行
			save.write('address,decimals,symbol\n')
			error.write('address,chain\n')

			# 遍历调用字段
			for token in tokens:
				coin_contract = w3.eth.contract(address=Web3.toChecksumAddress(token), abi=abi)
				try:
					decimals = coin_contract.functions.decimals().call()
					if not isinstance(decimals, int):
						raise Exception(f'decimals: {decimals} is not type int')
					data = ','.join([token, str(decimals), coin_contract.functions.symbol().call() or '""']) + '\n'
					save.write(data)
				except Exception as e:
					print(f'token: {token}, error====={e}')
					error.write(f'{token},{chain}\n')
					continue
		return save_path + save_name, error_path + save_name

	get_token_operator = PythonOperator(
		task_id=f'footprint_token_info_{chain}_erc20',
		python_callable=get_token_info,
		provide_context=True,
		retry_delay=datetime.timedelta(minutes=10),
		retries=3,
		dag=dag
	)
	return get_token_operator


def create_load_tasks(dag: models.DAG, chain: str, conf: dict) -> PythonOperator:
	def load(**kwargs):
		table = conf['erc_20_table']
		# 加载正确的链的数据
		import_gsc_to_bigquery_base(
			schema=get_schema('erc20_tokens'),
			uri=f'gs://us-east4-ethereum-event-par-f1b473b1-bucket/data/erc20_token/{chain}/*',
			bigquery_table_name=erc_20_stable_table + table + '_temp'
		)

		# 加载访问异常的数据
		import_gsc_to_bigquery_base(
			schema=get_schema('erc20_error_tokens'),
			uri=f'gs://us-east4-ethereum-event-par-f1b473b1-bucket/data/erc20_token/error/*',
			bigquery_table_name='xed-project-237404.footprint_etl.erc20_token_error'
		)

	load_operator = PythonOperator(
		task_id=f'footprint_load_{chain}_erc20',
		python_callable=load,
		provide_context=True,
		retry_delay=datetime.timedelta(minutes=10),
		retries=3,
		dag=dag
	)
	return load_operator


def valid_duplicate(conf):
	table = erc_20_stable_table + conf['erc_20_table']
	temp_table = table + '_temp'
	"""token数量不能重复"""
	sql = f"""
	select cast (
((select count(*) from (
		select
			contract_address,
			MIN(symbol) as symbol,
			count(*) as nums
		from `{temp_table}`
		group by contract_address
		having nums > 1
	)) = 0) as boolean)
"""
	return sql


def valid_count(conf):
	table = erc_20_stable_table + conf['erc_20_table']
	temp_table = table + '_temp'
	"""新载入的数量不能少于原有表"""
	sql = f""" 	select cast ((select count(*) from `{temp_table}`) >= (select count (*) from `{table}`) as boolean)
"""
	return sql


def create_verify_tasks(dag: models.DAG, chain: str, sql: str, verify_type: str) -> PythonOperator:
	verify_operator = BigQueryCheckOperator(
		task_id=f'footprint_verify_{verify_type}_{chain}_erc20',
		sql=sql,
		use_legacy_sql=False,
		retries=2,
		dag=dag)
	return verify_operator


def create_merge_tasks(dag: models.DAG, chain: str, conf: dict) -> PythonOperator:
	def merge(**kwargs):
		table = erc_20_stable_table + conf['erc_20_table']
		temp_table = table + '_temp'
		client = bigquery.Client()
		job_config = bigquery.QueryJobConfig(destination=(table))
		job_config.write_disposition = 'WRITE_TRUNCATE'
		source_sql = f"""select * from {temp_table}"""
		# Start the query, passing in the extra configuration.

		query_job = client.query(source_sql, job_config=job_config)  # Make an API request.
		query_job.result()  # Wait for the job to complete.

	merge_operator = PythonOperator(
		task_id=f'footprint_merge_{chain}_erc20',
		python_callable=merge,
		provide_context=True,
		retry_delay=datetime.timedelta(minutes=10),
		retries=3,
		dag=dag
	)

	return merge_operator

# create dags
def create_dags(
	chain,
	schedule_interval='0 0 * * *',
	load_start_date=datetime.datetime(2021, 11, 29)
) -> models.DAG:
	"""创建加载的dags"""
	# note: 这里面不需要重跑, 重跑是增量的.
	dag = models.DAG(
		dag_id=f'foot_print_{chain}_erc20_tokens',
		schedule_interval=schedule_interval,
		start_date=load_start_date,
		tags=['erc20tokens'],
		catchup=False,
		dagrun_timeout=datetime.timedelta(days=1)
	)
	conf = ChainConfig[chain]
	get_info_task = create_get_token_info_task(dag, chain, conf)
	load_task = create_load_tasks(dag, chain, conf)
	valid_count_task = create_verify_tasks(dag, chain, sql=valid_count(conf), verify_type='count')
	valid_duplicate_task = create_verify_tasks(dag, chain, sql=valid_duplicate(conf), verify_type='duplicate')
	merge_task = create_merge_tasks(dag, chain, conf)
	get_info_task >> load_task
	load_task >> valid_duplicate_task
	load_task >> valid_count_task
	valid_count_task >> merge_task
	valid_duplicate_task >> merge_task
	return dag


DAG_ETHEREUM = create_dags(CHAIN['ETHEREUM'])
DAG_BSC = create_dags(CHAIN['BSC'])
DAG_POLYGON = create_dags(CHAIN['POLYGON'])
DAG_ARBITRUM = create_dags(CHAIN['ARBITRUM'])
DAG_FANTOM = create_dags(CHAIN['FANTOM'])


# TODO: 1. 能够使用该脚本执行逻辑, 方向: 通过参数控制返回函数还是operator. 下面main为概念, 未实现
# if __name__ == '__main__':
# 	chain = CHAIN['ETHEREUM']
# 	conf = ChainConfig[chain]
# 	get_token_func = create_get_token_info_task(None, chain, conf, raw_func=True)
# 	get_token_func()
# 	load_func = create_load_tasks(None, chain, conf, raw_func=True)
# 	load_func()
# TODO: 2. 调整为按日增量, 减少计算的数据量. 思路为: 增加一个新表. 先判断里面是否有对应的业务, 若是没有该业务则全刷. 要是有, 则刷当天.再进行group操作
# TODO: 3. 调整文件路径, 去掉旧的erc20入口. 新增手动传输的入口.
