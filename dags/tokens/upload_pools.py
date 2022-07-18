import os

import pandas as pd
from google.cloud import bigquery

from config import project_config
from utils.import_gsc_to_bigquery import get_schema
import csv
import json


big_query_client = bigquery.Client()


def load_to_bigquery(table_id, csv_file, schema, file_format=bigquery.SourceFormat.CSV):
    job_config = bigquery.LoadJobConfig(
        source_format=file_format,
        autodetect=False,
        schema=schema,
        write_disposition='WRITE_TRUNCATE'
    )

    with open(csv_file, "rb") as source_file:
        job = big_query_client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.
    table = big_query_client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def csv_to_json(csv_path):
    json_file = open('pool_infos.json', 'w')

    with open(csv_path, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            row['stake_token'] = row['stake_token'].split('-')
            row['reward_token'] = row['reward_token'].split('-') if row['reward_token'] else []
            json_file.write(json.dumps(row) + '\n')


def upload_pools():
    for file_name in [
        'pool_infos'
    ]:
        table_name = 'footprint_etl.{}'.format(file_name)
        csv_path = './{}.csv'.format(file_name)
        csv_to_json(csv_path)

        json_path = './{}.json'.format(file_name)
        schema = get_schema(file_name)
        print(table_name, json_path)
        load_to_bigquery(table_name, json_path, schema, bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)


# 执行前注意，stake_token / reward_token 是数组格式，在csv中需要用 - 分开
if __name__ == '__main__':
    upload_pools()
