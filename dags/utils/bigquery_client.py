from utils import Constant
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

footprint_credentials = Credentials.from_service_account_file(filename='')
footprint_bigquery_client = bigquery.Client(credentials=footprint_credentials)
xed_project_credentials = Credentials.from_service_account_file(filename='')
xed_project_bigquery_client = bigquery.Client(credentials=xed_project_credentials)


bigquery_client = {
    Constant.BIGQUERY_CLIENT['XED_CLIENT']: '',
    Constant.BIGQUERY_CLIENT['FOOTPRINT_CLIENT']: ''
}


def get_bigquery_client(client_name):
    return bigquery_client[Constant.BIGQUERY_CLIENT['FOOTPRINT_CLIENT']] if client_name == Constant.BIGQUERY_CLIENT['FOOTPRINT_CLIENT'] else Constant.BIGQUERY_CLIENT['FOOTPRINT_CLIENT']
