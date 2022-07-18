import os

from google.cloud import bigquery

from config import project_config
from google.cloud.bigquery.job import QueryJobConfig

default_bucket_name = project_config.bigquery_bucket_name

client = bigquery.Client()


def query_to_table(query, project_id, dataset_id, table_id):
	conf = QueryJobConfig(
		destination=f'{project_id}.{dataset_id}.{table_id}',
		write_disposition='WRITE_TRUNCATE'
	)
	job = client.query(query, conf)
	# 等待执行结束
	job.result()


def export_table_to_csv(bucket_name, temp_dir_name, csv_name, project_id, dataset_id, table_id, big_result=False):
	destination_uri = f"gs://{bucket_name or default_bucket_name}/{temp_dir_name}/{csv_name}"
	if big_result:
		destination_uri += '*'
	dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
	table_ref = dataset_ref.table(table_id)

	extract_job = client.extract_table(
		table_ref,
		destination_uri,
		# Location must match that of the source table.
		location="US",
	)  # API request
	extract_job.result()  # Waits for job to complete.

	print(
		"Exported {}:{}.{} to {}".format(project_id, dataset_id, table_id, destination_uri)
	)

