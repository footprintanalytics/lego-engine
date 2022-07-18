import os
import pydash
import copy
import datetime
import pymongo
import pandas as pd
from config import project_config
from utils.export_gsc_from_bigquery import query_to_table, export_table_to_csv
from utils.gcs_util import GCSUtil
from utils.constant import PROJECT_PATH
from utils.query_bigquery import query_bigquery

EXPORT_OUTPUT_DEFAULT_PATH = PROJECT_PATH+'/../data/tofu_portfolio_transfer/csv/'


class BQToMongoConfig:

	def _check_path_exists(self, export_local_path, temp_dir_name):
		path = export_local_path or EXPORT_OUTPUT_DEFAULT_PATH
		path = path + temp_dir_name + '/'
		if not os.path.exists(path):
			"""if path not exists, then create path"""
			os.mkdir(path)
		return path

	def __init__(
			self,
			mongodb_collection_name,
			sql=None,
			project_id=project_config.project_id,
			project_name='tofu_portfolio',
			execution_time='',
			task_name='',
			temp_bq_dataset_id='tofu_portfolio',
			temp_bq_table_id=None,
			bucket_name='tofu_portfolio',
			temp_dir_name=None,
			csv_name='',
			export_local_path=None,
			mongodb_uri=project_config.mongodb_tofu_portfolio_uri,
			mongodb_db_name='tofu_portfolio',
			mongodb_import_value_type='',
			mongodb_delete_exists_query=None,
			mongodb_verify_query=None,
			bq_verify_query=None,
			enable_verify=True,
			big_result=False
	):
		# task 部分
		self.project_id = project_id
		self.project_name = project_name
		self.execution_time = execution_time
		self.task_name = task_name

		# bigquery 部分
		self.temp_bq_dataset_id = temp_bq_dataset_id
		self.temp_bq_table_id = temp_bq_table_id or mongodb_collection_name + '_temp'
		self.sql = sql
		self.big_result = big_result

		# cloud storage
		self.bucket_name = bucket_name
		self.temp_dir_name = temp_dir_name or mongodb_collection_name
		self.csv_name = csv_name

		# local storage config
		self.export_local_path = self._check_path_exists(export_local_path, self.temp_dir_name)
		self.local_csv_path = self.export_local_path + self.csv_name

		# mongodb basic config
		self.mongodb_uri = mongodb_uri
		self.mongodb_db_name = mongodb_db_name
		self.mongodb_collection_name = mongodb_collection_name
		self.mongodb_collection = pymongo.MongoClient(self.mongodb_uri)[self.mongodb_db_name][self.mongodb_collection_name]

		# mongodb drop duplicate
		self._mongodb_delete_exists_query = mongodb_delete_exists_query

		# verify
		self.enable_verify = enable_verify
		self.bq_verify_query = bq_verify_query if bq_verify_query else f'select count(*) from `{self.project_id}.{self.temp_bq_dataset_id}.{self.temp_bq_table_id}`'
		self.mongodb_verify_query = mongodb_verify_query if mongodb_verify_query is not None else (mongodb_delete_exists_query or {})

		# csv change type
		self.mongodb_import_value_type = mongodb_import_value_type


	@property
	def mongodb_delete_exists_query(self):
		"""避免内容被修改"""
		if self._mongodb_delete_exists_query is not None:
			return copy.deepcopy(self._mongodb_delete_exists_query)
		return self._mongodb_delete_exists_query


class BigqeryToMongo:
	project_id = project_config.project_id

	def __init__(self, conf: BQToMongoConfig):
		self.conf = conf

	def airflow_dag_params(self):
		dag_params = {
			"dag_id": "tofu_portfolio_bq_to_mongo_{}_dag".format(self.conf.task_name),
			"catchup": False,
			"schedule_interval": self.conf.execution_time,
			"description": "{}_dag".format(self.conf.task_name),
			"default_args": {
				'owner': 'airflow',
				'depends_on_past': False,
				'retries': 1,
				'retry_delay': datetime.timedelta(minutes=10),
				'start_date': datetime.datetime(2021, 9, 14)
			},
			"dagrun_timeout": datetime.timedelta(days=30)
		}
		print('dag_params', dag_params)
		return dag_params

	def _query_to_table(self):
		query_to_table(self.conf.sql, project_id=self.conf.project_id, dataset_id=self.conf.temp_bq_dataset_id, table_id=self.conf.temp_bq_table_id)

	def _export_table_to_csv(self):
		export_table_to_csv(
			bucket_name=self.conf.bucket_name,
			temp_dir_name=self.conf.temp_dir_name,
			csv_name=self.conf.csv_name,
			project_id=self.conf.project_id,
			dataset_id=self.conf.temp_bq_dataset_id,
			table_id=self.conf.temp_bq_table_id,
			big_result=self.conf.big_result)

	def _download_csv_from_gsc(self):
		GCSUtil(self.conf.bucket_name).download_from_gsc(
			source_blob_name=f'{self.conf.temp_dir_name}/{self.conf.csv_name}',
			destination_file_name=self.conf.local_csv_path)


	@staticmethod
	def delete_exists_query(connection, query: None):
		if query is not None:
			connection.delete_many(query)

	def _delete_exists_query(self):
		self.delete_exists_query(self.conf.mongodb_collection, self.conf.mongodb_delete_exists_query)


	@staticmethod
	def import_from_csv(connection, csv_path, fields_type_changes=None):
		data = pd.read_csv(csv_path)
		if fields_type_changes:
			datetime_keys = pydash.objects.pick_by(fields_type_changes, lambda v: v == 'datetime').keys()
			for key in datetime_keys:
				data[key] = pd.to_datetime(data[key])
			other_type = pydash.objects.omit(fields_type_changes, *datetime_keys)
			other_keys = list(other_type.keys())
			data.loc[:, other_keys] = data.loc[:, other_keys].astype(other_type)
		connection.insert_many(data.to_dict(orient='records'))

	def _import_from_csv(self):
		self.import_from_csv(
			connection=self.conf.mongodb_collection,
			csv_path=self.conf.local_csv_path,
			fields_type_changes=self.conf.mongodb_import_value_type)

	def verify(self):
		if self.conf.enable_verify:
			collection_count = self.conf.mongodb_collection.count(self.conf.mongodb_verify_query)
			bq_count = query_bigquery(self.conf.bq_verify_query).iloc[0, 0]
			assert collection_count == bq_count, f'collection count:{collection_count} should be equal to bigquery count: {bq_count}, delta is :{bq_count-collection_count}'

	def airflow_steps(self):
		return [
			self._query_to_table,
			self._export_table_to_csv,
			self._download_csv_from_gsc,
			self._delete_exists_query,
			self._import_from_csv,
			self.verify
		]
