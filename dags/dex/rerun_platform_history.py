import pandas as pd

from common.common_dex_model import DexModel
from dex.create_all_view import view_creator_runner, all_project
from utils.date_util import DateUtil
import datetime
from joblib import Parallel, delayed
from tqdm import tqdm


def history_fixer(platform_class, business_type):
	assert datetime.datetime.now().hour not in range(10, 13) and datetime.datetime.now().weekday() != 4, '不允许在10-13点或者周五执行刷数据脚本, 要么就注释我!'
	assert business_type in DexModel.business_type.keys(), f'business_type must in DexModel.business_type.keys(): {DexModel.business_type.keys()}'
	pool = platform_class()
	business = pool.get_business_type(pool.business_type[business_type])
	business.delete_history_data(is_test=False)
	business.parse_history_data()
	for dates in pd.date_range(start=pool.history_date, end=DateUtil.utc_24h_ago().strftime('%Y-%m-%d')).strftime(
			'%Y-%m-%d').to_list():
		business.run_daily_job(date_str=dates)
	business.create_all_data_view()
	view_creator_runner(business_type)


def history_fixer_all_business(platform_class):
	try:
		history_fixer(platform_class, 'swap')
	except Exception as e:
		print(e)

	try:
		history_fixer(platform_class, 'add_liquidity')
	except Exception as e:
		print(e)

	try:
		history_fixer(platform_class, 'remove_liquidity')
	except Exception as e:
		print(e)


if __name__ == '__main__':
	Parallel(n_jobs=6)(delayed(history_fixer_all_business)(item) for item in tqdm(all_project))

	"""执行demo"""
	# from dex.ethereum.tokenlon.tokenlon import TokenlonDex
	# history_fixer(TokenlonDex, DexModel.business_type['swap'])

