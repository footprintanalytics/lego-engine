import subprocess
from datetime import datetime,timedelta,date
from migrate_data.tag.import_from_mongo_tag import ImportFromMongoTag
import json
import os

class ImportFromMongoTagYesterday(ImportFromMongoTag):

    def __init__(self):
        super().__init__()
        self.task_name = 'migrate_tag_mongo_data_yesterday'
        self.task_airflow_execution_time = '0 1 * * *'
        self.increment_schema = ('entity_tag')
        self.file_date = (datetime.today()+timedelta(-1)).strftime('%Y-%m-%d')
        # self.file_date = (datetime(2021,8,28)+timedelta(-1)).strftime('%Y-%m-%d')

    def gen_schema_info(self):
        for file in os.listdir(self.schema_json_path):
            file_name = file.split('.json')[0]
            if not file_name in self.increment_schema:
                continue
            file_path = os.path.join(self.schema_json_path, file)
            fields = []
            with open(file_path) as f:
                data = json.load(f)
                for i in data:
                    fields.append(i.get('description'))
            # field = ','.join(fields)
            self.schema_info.append({'name': file_name, 'fields':fields})

    def get_export_query(self,name):
        # d = date(2021,8,28)
        d = date.today()
        return {} if not name in self.increment_schema else {"updatedAt": {'$gte':  datetime(d.year, d.month, d.day)+timedelta(-1),'$lt': datetime(d.year, d.month, d.day)}}

    # def export_csv(self, name, field, source_path):
    #     c_name = self.table_reflection.get(name)
    #     d = date.today()
    #     start_d = (datetime(d.year, d.month, d.day)+timedelta(-1)).strftime('%Y-%m-%d') +'T00:00:00.000Z'
    #     end_d = (datetime(d.year, d.month, d.day)).strftime('%Y-%m-%d') +'T00:00:00.000Z'
    #     query = {'updatedAt':{'$gte':{'$date':start_d}, '$lt':{'$date':end_d}}}
    #     query_str = json.dumps(query)
    #     print(query_str)
    #     subprocess.call(f'''mongoexport --uri="{self.mongodb_uri}" --collection="{c_name}" --query='{query_str}' --type=csv --fields="{field}" --out="{source_path}"''',shell=True)
