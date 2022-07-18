from __future__ import print_function

from glob import glob
import logging
import os

from polygon_event_etl.build_parse_dag import build_parse_dag
from polygon_event_etl.variables import read_parse_dag_vars

DAGS_FOLDER = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')
table_definitions_folder = os.path.join(DAGS_FOLDER, 'resources/stages/parse/polygon_table_definitions/*')

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

var_prefix = 'polygon_'

for folder in glob(table_definitions_folder):
    dataset = folder.split('/')[-1]

    dag_id = f'polygon_parse_{dataset}_dag'
    logging.info(folder)
    logging.info(dataset)
    globals()[dag_id] = build_parse_dag(
        dag_id=dag_id,
        dataset_folder=folder,
        **read_parse_dag_vars(
            var_prefix=var_prefix,
            dataset=dataset,
            schedule_interval='0 12 * * *',
            parse_destination_dataset_project_id='footprint-etl'
        )
    )
