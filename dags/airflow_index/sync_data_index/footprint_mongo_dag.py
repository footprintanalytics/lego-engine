from utils.build_dag_util import BuildDAG
from migrate_data.footprint.import_from_mongo_footprint import ImportFromMongoFootprint
from migrate_data.tag.import_from_mongo_tag import ImportFromMongoTag
from migrate_data.tag.import_from_mongo_tag_yesterday import ImportFromMongoTagYesterday
from datetime import datetime, timedelta

# DAG = BuildDAG().build_dag_with_ops(dag_params=importFromMongo.airflow_dag_params(), ops=importFromMongo.airflow_steps())
importFromMongoFootprint = ImportFromMongoFootprint()
importFromMongoTag = ImportFromMongoTag()
importFromMongoTagYesterday = ImportFromMongoTagYesterday()

DAG1 = BuildDAG().build_dag(dag_params=importFromMongoFootprint.airflow_dag_params(), dag_task_params=importFromMongoFootprint.airflow_dag_task_params())
DAG2 = BuildDAG().build_dag(dag_params=importFromMongoTag.airflow_dag_params(), dag_task_params=importFromMongoTag.airflow_dag_task_params())
DAG3 = BuildDAG().build_dag(dag_params=importFromMongoTagYesterday.airflow_dag_params(), dag_task_params=importFromMongoTagYesterday.airflow_dag_task_params())
