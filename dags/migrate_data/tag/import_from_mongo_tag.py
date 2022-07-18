import subprocess
import os
from datetime import datetime,timedelta,date
from migrate_data.import_from_mongo_base import ImportFromMongoBase
from config import project_config
from models.tag import get_model_instance
import json


class ImportFromMongoTag(ImportFromMongoBase):

    def __init__(self):
        super().__init__()
        self.mongodb_uri = project_config.mongodb_tag_uri
        self.temp_csv_dir = os.path.join(project_config.dags_folder, 'migrate_data/tag/csv')
        self.bucket_name = project_config.bigquery_bucket_name
        self.schema_json_path = os.path.join(project_config.dags_folder, 'migrate_data/tag/schemas')
        self.task_name = 'migrate_tag_mongo_data'
        self.task_airflow_execution_time = '*/30 * * * *'
        self.d_path_prefix = 'tag_'
        self.increment_schema = ('entity_tag')
        self.file_date = (datetime.today()).strftime('%Y-%m-%d')
        self.table_reflection = {'entity_tag':'entityTag','archive_entity_tag':'archiveEntityTag'}

    def get_model_instance(self,name):
        return get_model_instance(name)

    def get_export_query(self,name):
        d = date.today()
        return {} if not name in self.increment_schema else {"updatedAt": {'$gte':  datetime(d.year, d.month, d.day),'$lt':  datetime(d.year, d.month, d.day) + timedelta(1)}}

    # def export_csv(self, name, field, source_path):
    #     c_name = self.table_reflection.get(name)
    #     if  name in self.increment_schema:
    #         d = date.today()
    #         start_d = datetime(d.year, d.month, d.day).strftime('%Y-%m-%d') +'T00:00:00.000Z'
    #         end_d = (datetime(d.year, d.month, d.day)+timedelta(1)).strftime('%Y-%m-%d') +'T00:00:00.000Z'
    #         query = {'updatedAt':{'$gte':{'$date':start_d}, '$lt':{'$date':end_d}}}
    #         query_str = json.dumps(query)
    #         print(query_str)
    #         subprocess.call(f'''mongoexport --uri="{self.mongodb_uri}" --collection="{c_name}" --query='{query_str}' --type=csv --fields="{field}" --out="{source_path}"''',shell=True)
    #     else:
    #         subprocess.call(f'''mongoexport --uri="{self.mongodb_uri}" --collection="{c_name}" --type=csv --fields="{field}" --out="{source_path}"''',shell=True)

    def get_gsc_file_path(self,schema_name):
        destination_file_path = f'{self.d_path_prefix}{schema_name}'
        if schema_name in self.increment_schema:
            return '{path}/{date}_{name}.csv'.format(path=destination_file_path, name=schema_name,date=self.file_date)
        else:
            return '{path}/{name}.csv'.format(path=destination_file_path, name=schema_name)


