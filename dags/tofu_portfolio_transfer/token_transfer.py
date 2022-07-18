import os
import datetime
from utils.date_util import DateUtil
from tofu_portfolio_transfer.base import BigqeryToMongo, BQToMongoConfig


def sql_maker(time_format_start: str, time_format_end: str):
	query = f"""SELECT
  token_transfers.transaction_hash,
  token_transfers.block_timestamp,
  transactions.gas AS gas,
  transactions.gas_price AS gas_price,
  token_transfers.to_address,
  token_transfers.token_address,
  token_transfers.value,
  token_transfers.from_address,
  token_transfers.block_number,
FROM
  (select * from `bigquery-public-data.crypto_ethereum.token_transfers` WHERE block_timestamp BETWEEN '{time_format_start}' and '{time_format_end}') AS token_transfers
JOIN
  (select gas, gas_price, `hash` from `bigquery-public-data.crypto_ethereum.transactions` WHERE block_timestamp BETWEEN '{time_format_start}' and '{time_format_end}') AS transactions
ON
  token_transfers.transaction_hash=transactions.hash"""
	return query


def verify_sql_maker(time_format_end: str, platforms: [str] = []):
	platform_count = [f"" for i in platforms]
	union_count = '\nunion all \n'.join(platform_count)
	query = f'select sum(c) from (\n{union_count}\n)'
	return query


def token_transfer_config_creator(start: datetime.datetime = None, end: datetime.datetime = None):
	if end is None:
		end = DateUtil.utc_start_of_date()
	if start is None:
		start = (end - datetime.timedelta(days=1))
	time_format_start = start.strftime('%Y-%m-%d %H:%M:%S')
	time_format_end = end.strftime('%Y-%m-%d %H:%M:%S')
	sql = sql_maker(**{
		'time_format_start': time_format_start,
		'time_format_end': time_format_end
	})
	verify_sql = f"select count(*) as c from `bigquery-public-data.crypto_ethereum.token_transfers` where block_timestamp <= '{time_format_end}'"
	TransactionConfig = BQToMongoConfig(
		project_name=None,
		# 每日定时任务执行时间
		execution_time='30 3 * * *',  # UTC 时间
		# airflow 的task name，也会是 sql table 的name
		task_name='token_transfer',
		sql=sql,
		export_local_path=None,
		csv_name=f'{time_format_start}-{time_format_end}',
		mongodb_import_value_type={
			'block_timestamp': 'datetime',
			'gas': 'int',
			'gas_price': 'int',
			'value': 'double',
			'block_number': 'int'
		},
		mongodb_delete_exists_query={'block_timestamp': {'$gte': start, '$lte': end}},
		mongodb_collection_name='token_transfers',
		mongodb_verify_query={'block_timestamp': {'$lte': end}},
		enable_verify=False,
		bq_verify_query=verify_sql
	)
	return TransactionConfig


if __name__ == '__main__':
	conf = token_transfer_config_creator()
	steps = BigqeryToMongo(conf).airflow_steps()
