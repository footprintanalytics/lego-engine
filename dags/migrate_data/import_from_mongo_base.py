import subprocess
import os
import pandas
import json
from datetime import timedelta, datetime
from config import project_config
from utils.upload_csv_to_gsc import upload_csv_to_gsc
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery_base
from utils.bigquery_utils import read_bigquery_schema_from_file
import sys
import json
from models.footprint_prod import get_model_instance
import pydash
import copy


class ImportFromMongoBase(object):

    def __init__(self):
        self.mongodb_uri = project_config.mongodb_footprint_uri
        self.temp_csv_dir = os.path.join(project_config.dags_folder, 'migrate_data/footprint/csv')
        self.dataset_id = 'xed-project-237404.metabase'
        self.bucket_name = project_config.bigquery_bucket_name
        self.schema_json_path = os.path.join(project_config.dags_folder, 'migrate_data/footprint/schemas')
        self.task_name = 'migrate_footprint_mongo_data'
        self.task_airflow_execution_time = '*/30 * * * *'
        self.schema_info = []
        self.d_path_prefix = 'footprint_prod_'

    def gen_dir(self):
        if not os.path.exists(self.temp_csv_dir):
            os.mkdir(self.temp_csv_dir)

    def gen_schema_info(self):
        for file in os.listdir(self.schema_json_path):
            file_path = os.path.join(self.schema_json_path, file)
            fields = []
            with open(file_path) as f:
                data = json.load(f)
                for i in data:
                    fields.append(i.get('description'))
            # field = ':1,'.join(fields)
            self.schema_info.append({'name': file.split('.json')[0], 'fields': fields})

    def get_export_query(self, name):
        return {}

    def get_model_instance(self, name):
        return get_model_instance(name)

    def format_value_dict_flatten(self, info):

        def _format(dict_obj):
            r_dict_obj = copy.deepcopy(dict_obj)
            for key in dict_obj:
                if type(dict_obj[key]) != type({}):
                    continue
                for c_key in dict_obj[key]:
                    r_key = '{}.{}'.format(key, c_key)
                    r_dict_obj[r_key] = pydash.get(dict_obj[key], c_key)
            return r_dict_obj

        return list(map(lambda n: _format(n), info))

    def export_csv(self, name, fields, source_path):
        model_instance = self.get_model_instance(name)
        projection = {}
        for field in fields:
            projection[field] = 1
        print('projection====', projection)
        query = self.get_export_query(name)
        print('query====', query)
        info = model_instance.find_list(query, projection=projection)
        print('info====', info)
        info = self.format_value_dict_flatten(info)
        df = pandas.DataFrame(info, columns=fields)
        print(df.head())
        # df['_id'] = str(df['_id'])
        df.to_csv(source_path, index=False)

        # subprocess.call(
        #     f'mongoexport --uri="{self.mongodb_uri}" --collection="{name}" --type=csv --fields="{field}" --out="{source_path}"',
        #     shell=True)

    def trans_objectid_to_string(self, source_file_path):
        if sys.platform == 'darwin':
            subprocess.call(["sed", "-i", "", r"s/ObjectId(\([[:alnum:]]*\))/\1/g", f"{source_file_path}"])
        else:
            subprocess.call(["sed", "-i", r"s/ObjectId(\([[:alnum:]]*\))/\1/g", f"{source_file_path}"])

    def get_schema(self, schema_name):
        schema_path = f'{self.schema_json_path}/{schema_name}.json'
        schema = read_bigquery_schema_from_file(schema_path)
        return schema

    def get_gsc_file_path(self, schema_name):
        destination_file_path = f'{self.d_path_prefix}{schema_name}'
        return '{path}/{name}.csv'.format(path=destination_file_path, name=schema_name)

    def upload_csv(self, source_file_path, schema_name):
        destination_file_path = self.get_gsc_file_path(schema_name)
        upload_csv_to_gsc(source_file_path, destination_file_path)

    def load_csv_to_bq(self, schema_name, ):
        schema = self.get_schema(schema_name)
        gsc_folder = self.d_path_prefix + schema_name
        table_name = f'{self.dataset_id}.{self.d_path_prefix}{schema_name}'
        uri = "gs://{bucket_name}/{gsc_folder}/*.csv".format(bucket_name=self.bucket_name, gsc_folder=gsc_folder)
        import_gsc_to_bigquery_base(schema, uri, bigquery_table_name=table_name)

    def remove_temp_csv(self):
        pass

    def exc(self):
        self.gen_dir()
        self.gen_schema_info()
        for info in self.schema_info:
            schema_name, schema_fields = info.get('name'), info.get('fields')
            print('info:', schema_name, schema_fields)
            source_file_path = os.path.join(self.temp_csv_dir, f'{schema_name}.csv')
            self.export_csv(schema_name, schema_fields, source_file_path)
            print('export_csv success')
            # self.trans_objectid_to_string(source_file_path)
            self.upload_csv(source_file_path, schema_name)
            self.load_csv_to_bq(schema_name)

    def python_callable(self):
        self.exc()

    def airflow_dag_params(self):
        default_dag_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2021, 7, 1)
        }

        dag_params = {
            "dag_id": "{task_name}_dag".format(task_name=self.task_name),
            "catchup": False,
            "schedule_interval": self.task_airflow_execution_time,
            "description": "{task_name} dag".format(task_name=self.task_name),
            "default_args": default_dag_args,
            "dagrun_timeout": timedelta(hours=3)
        }
        return dag_params

    def airflow_dag_task_params(self):
        dag_task_params = [
            {
                "task_id": self.task_name,
                "python_callable": self.python_callable,
                "execution_timeout": timedelta(hours=3)
            }
        ]
        return dag_task_params
