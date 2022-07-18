import json
import logging
import os
from http import HTTPStatus

from google.cloud import bigquery
from google.api_core.exceptions import Conflict, NotFound
from utils.common import read_file
from utils.constant import PROJECT_PATH


def submit_bigquery_job(job, configuration):
    try:
        logging.info('Creating a job: ' + json.dumps(configuration.to_api_repr()))
        result = job.result()
        logging.info(result)
        assert job.errors is None or len(job.errors) == 0
        return result
    except Exception as ex:
        logging.info(ex)
        raise


def create_or_update_view(view_name: str, view_sql: str):
    view = bigquery.Table(view_name)
    view.view_query = view_sql
    print(view_sql)
    client = bigquery.Client()
    try:
        view = client.create_table(view)
        print(f"Created {view.table_type}: {str(view.reference)}")
    except Exception as ex:
        if ex.code == HTTPStatus.CONFLICT:
            view = client.update_table(view, ["view_query"])
            print(f"Updated {view.table_type}: {str(view.reference)}")
        else:
            logging.info(ex)
            raise


def read_bigquery_schema_from_file(filepath):
    file_content = read_file(filepath)
    json_content = json.loads(file_content)
    return read_bigquery_schema_from_json_recursive(json_content)


def read_bigquery_schema_from_json_recursive(json_schema):
    """
    CAUTION: Recursive function
    This method can generate BQ schemas for nested records
    """
    result = []
    for field in json_schema:
        if field.get('type').lower() == 'record' and field.get('fields'):
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description'),
                fields=read_bigquery_schema_from_json_recursive(field.get('fields'))
            )
        else:
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')
            )
        result.append(schema)
    return result


def create_view(bigquery_client, sql, table_ref, description=None):
    table = bigquery.Table(table_ref)
    table.view_query = sql

    if description is not None:
        table.description = description

    logging.info('Creating view: ' + json.dumps(table.to_api_repr()))

    try:
        table = bigquery_client.create_table(table)
    except Conflict:
        # https://cloud.google.com/bigquery/docs/managing-views
        table = bigquery_client.update_table(table, ['view_query'])

    if isinstance(table_ref, str):
        assert f"{table.project}.{table.dataset_id}.{table.table_id}" == table_ref
    else:
        assert table.table_id == table_ref.table_id


def does_table_exist(bigquery_client, table_ref):
    try:
        table = bigquery_client.get_table(table_ref)
    except NotFound:
        return False
    return True


def query(bigquery_client, sql, destination=None, priority=bigquery.QueryPriority.INTERACTIVE):
    job_config = bigquery.QueryJobConfig()
    job_config.destination = destination
    job_config.priority = priority
    logging.info('Executing query: ' + sql)
    query_job = bigquery_client.query(sql, location='US', job_config=job_config)
    submit_bigquery_job(query_job, job_config)
    assert query_job.state == 'DONE'


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


def query_and_save_bq_schema(project='footprint-etl-internal', source_sql=None, destination=None,
                             time_partitioning_name=None,
                             write_disposition='WRITE_TRUNCATE'):
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig(destination=(destination))
    job_config.write_disposition = write_disposition
    if time_partitioning_name is not None:
        job_config.time_partitioning = get_time_partitioning(time_partitioning_name)
        # Start the query, passing in the extra configuration.
    print("Query string ====>", source_sql)
    query_job = client.query(source_sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    print("Query results loaded to the table {}".format(destination))


def share_dataset_all_users_read(bigquery_client, full_dataset_name):
    bigquery.AccessEntry.ENTITY_TYPES = ["userByEmail", "groupByEmail", "domain", "specialGroup", "view", "iamMember"]

    role = 'READER'
    entity_type = 'iamMember'
    entity_id = 'allUsers'

    dataset = bigquery_client.get_dataset(full_dataset_name)
    entries = list(dataset.access_entries)
    is_shared = False
    for entry in entries:
        if entry.role == role and entry.entity_type == entity_type and entry.entity_id == entity_id:
            is_shared = True

    if not is_shared:
        entry = bigquery.AccessEntry(
            role=role,
            entity_type=entity_type,
            entity_id=entity_id,
        )
        entries.append(entry)
        dataset.access_entries = entries
        dataset = bigquery_client.update_dataset(dataset, ["access_entries"])
        logging.info('Updated dataset permissions')
    else:
        logging.info('The dataset is already shared')


def show_dataset_list(project_id='xed-project-237404'):
    client = bigquery.Client(project=project_id)
    return list(client.list_datasets())


def show_tale_list(project_id, dataset_id):
    client = bigquery.Client(project=project_id)
    return list(client.list_tables(dataset_id))


def get_bigquery_client(project_id: str = 'footprint-etl-internal'):
    return bigquery.Client(project='footprint-etl-internal')
