import os

from google.cloud import bigquery
from utils.common import read_file
from utils.constant import PROJECT_PATH
import json

from config import project_config
from utils.bigquery_utils import read_bigquery_schema_from_file

data_base = project_config.bigquery_etl_database


def load_csv_to_bigquery(table_id, csv_file, schema):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def import_gsc_to_bigquery(
        name,
        custom_data_base=None,
        schema_name=None,
        project_id=None,
        file_format='csv',
        bucket_name=None,
        gsc_folder=None
):
    if not custom_data_base:
        custom_data_base = data_base
    if not project_id:
        project_id = 'xed-project-237404'
    if not bucket_name:
        bucket_name = project_config.bigquery_bucket_name
    if not gsc_folder:
        gsc_folder= name

    client = bigquery.Client()
    bigquery_table_name = '{project_id}.{data_base}.{name}'.format(
        project_id=project_id,
        data_base=custom_data_base,
        name=name
    )
    job_config = bigquery.LoadJobConfig()
    uri = "gs://{bucket_name}/{gsc_folder}/*.{file_format}".format(bucket_name=bucket_name, gsc_folder=gsc_folder, file_format=file_format)
    schema = get_schema(schema_name or name)
    time_partitioning = get_time_partitioning(schema_name or name)
    print("Loaded table time_partitioning {}".format(time_partitioning))
    job_config.time_partitioning = time_partitioning
    if file_format == 'csv':
        job_config.skip_leading_rows = 1
    job_config.schema = schema
    job_config.source_format = bigquery.SourceFormat.CSV if file_format == 'csv' else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = 'WRITE_TRUNCATE'
    load_job = client.load_table_from_uri(
        uri, bigquery_table_name, job_config=job_config
    )
    load_job.result()

    destination_table = client.get_table(bigquery_table_name)
    print("Loaded {} rows.".format(destination_table.num_rows))


def get_schema(schema_name):
    dags_folder = PROJECT_PATH
    schema_path = os.path.join(dags_folder, 'resources/stages/raw/schemas/{schema_name}.json'.format(schema_name=schema_name))
    schema = read_bigquery_schema_from_file(schema_path)
    return schema


def get_path_schema(schema_name):
    dags_folder = PROJECT_PATH
    schema_path = os.path.join(dags_folder, '{schema_name}'.format(schema_name=schema_name))
    schema = read_bigquery_schema_from_file(schema_path)
    return schema


def get_time_partitioning(schema_name):
    dags_folder = PROJECT_PATH
    time_partitioning_path = os.path.join(dags_folder, 'resources/stages/raw/time_partitioning/{schema_name}.json'.format(schema_name=schema_name))
    if os.path.exists(time_partitioning_path) is False:
        return None
    file_content = read_file(time_partitioning_path)
    time_partitioning = json.loads(file_content)
    return bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,  # 兼容其他配置
        field=time_partitioning.get('field')
    )


def import_gsc_to_bigquery_base(
        schema,
        uri,
        bigquery_table_name,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition='WRITE_TRUNCATE',
        name=None,
        time_partitioning_field=None
):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = skip_leading_rows
    job_config.schema = schema

    time_partitioning = None
    if name:
        time_partitioning = get_time_partitioning(name)
    elif time_partitioning_field:
        time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,  # 兼容其他配置
            field=time_partitioning_field
        )

    print("Loaded table time_partitioning {}".format(time_partitioning))
    job_config.time_partitioning = time_partitioning
    job_config.source_format = source_format
    job_config.write_disposition = write_disposition
    load_job = client.load_table_from_uri(
        uri, bigquery_table_name, job_config=job_config
    )
    load_job.result()
    destination_table = client.get_table(bigquery_table_name)
    print("Loaded {} rows to {}.".format(destination_table.num_rows, bigquery_table_name))
