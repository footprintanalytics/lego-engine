from config import project_config
from utils.dune_request import DuneQuery
import os
from utils.import_gsc_to_bigquery import import_gsc_to_bigquery
from utils.upload_csv_to_gsc import upload_csv_to_gsc

class Bridges:
    def __init__(self, query_id, project_name, dags_folder):
        self.dune = DuneQuery()
        self.project_name = project_name
        self.dags_folder = dags_folder
        self.query_id = query_id

    def get_data(self):
        data = self.dune.query_result(self.query_id)
        return data

    def save_data(self):
        pass

    def handle_write_data_to_csv(self):
        pass

    def get_stats_csv_name(self):
        return os.path.join(self.dags_folder, '../data/{}.csv'.format(self.project_name))

    def handle_upload_csv_to_gsc(self):
        source_csv_file = self.get_stats_csv_name()
        destination_file_path = '{name}/{name}.csv'.format(name=self.project_name)
        upload_csv_to_gsc(source_csv_file, destination_file_path)
        print(source_csv_file)
        print(destination_file_path)

    def handle_import_gsc_csv_to_bigquery(self):
        import_gsc_to_bigquery(name=self.project_name)
        import_gsc_to_bigquery(name=self.project_name, project_id=project_config.migration_project_id, custom_data_base=project_config.migration_bigquery_struct_data)