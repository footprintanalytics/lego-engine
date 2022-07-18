import datetime
from utils.date_util import DateUtil
from tofu_portfolio_transfer.base import BigqeryToMongo, BQToMongoConfig


def sql_maker(time_format_start: str, time_format_end: str, platforms: [str] = []):
	query = '\nunion all \n'.join([
		f"""
	SELECT
		transaction_hash,
		block_timestamp,
		op_user,
		contract_address,
		gas,
		gas_price,
		to_address,
		token_address,
		token_symbol,
		operation,
		value,
		'{i}' AS platform
	FROM
		`xed-project-237404.footprint_etl.{i}_pool_transactions_all`
	WHERE
		block_timestamp BETWEEN '{time_format_start}' AND '{time_format_end}'""" for i in platforms])
	return query


def verify_sql_maker(time_format_end: str, platforms: [str] = []):
	platform_count = [f"select count(*) as c from `xed-project-237404.footprint_etl.{i}_pool_transactions_all` where block_timestamp <= '{time_format_end}'" for i in platforms]
	union_count = '\nunion all \n'.join(platform_count)
	query = f'select sum(c) from (\n{union_count}\n)'
	return query


def transaction_config_creator(start: datetime.datetime = None, end: datetime.datetime = None, platforms=[]):
	if end is None:
		end = DateUtil.utc_start_of_date()
	if start is None:
		start = (end - datetime.timedelta(days=1))
	time_format_start = start.strftime('%Y-%m-%d %H:%M:%S')
	time_format_end = end.strftime('%Y-%m-%d %H:%M:%S')
	if not platforms:
		platforms = ['convex', 'curve_v1', 'curve_v2', 'idle']
	sql = sql_maker(**{
		'platforms': platforms,
		'time_format_start': time_format_start,
		'time_format_end': time_format_end
	})
	verify_sql = verify_sql_maker(**{
		'platforms': platforms,
		'time_format_end': time_format_end
	})
	TransactionConfig = BQToMongoConfig(
		project_name=None,
		# 每日定时任务执行时间
		execution_time='30 3 * * *',  # UTC 时间
		# airflow 的task name，也会是 sql table 的name
		task_name='pool_transaction',
		sql=sql,
		export_local_path=None,
		csv_name=f'{time_format_start}-{time_format_end}',
		mongodb_import_value_type={
			'block_timestamp': 'datetime',
			'gas': 'int',
			'gas_price': 'int',
			'value': 'double'},
		mongodb_delete_exists_query={'block_timestamp': {'$gte': start, '$lte': end}, 'platform': {'$in': platforms}},
		mongodb_collection_name='pool_transactions',
		mongodb_verify_query={'block_timestamp': {'$lte': end}, 'platform': {'$in': platforms}},
		bq_verify_query=verify_sql
	)
	return TransactionConfig


if __name__ == '__main__':
	platforms = []
	conf = transaction_config_creator(start=datetime.datetime(2010, 1, 1), platforms=platforms)
	steps = BigqeryToMongo(conf).airflow_steps()
