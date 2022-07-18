from __future__ import print_function

from glob import glob
import logging
import os

from ethereum_event_etl.build_parse_dag import build_parse_dag
from ethereum_event_etl.variables import read_parse_dag_vars

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/table_definitions/*')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'ethereum_'


def get_schedule_time(_dataset):
    dataset_len = len(_dataset)
    start_time = 15
    add_time = dataset_len % 3
    _schedule_time = start_time + add_time
    return '0 {schedule_time} * * *'.format(schedule_time=_schedule_time)


for folder in glob(table_definitions_folder):
    dataset = folder.split('/')[-1]

    dag_id = f'ethereum_parse_{dataset}_dag'
    logging.info(folder)
    logging.info(dataset)
    schedule_time = get_schedule_time(dataset)
    globals()[dag_id] = build_parse_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        **read_parse_dag_vars(
            var_prefix=var_prefix,
            dataset=dataset,
            schedule_interval=schedule_time,
            parse_destination_dataset_project_id='footprint-etl'
        )
    )
