from utils.build_dag_util import BuildDAG
from defi360.validation import DeFi360Validation
import requests
import pydash
import datetime


class DeFi360TokenBalances:
    dao_backend_url = 'https://dao-prod.internal.footprint.network/api/v2/token/run_token_balance'

    def post_dao_backend(self):
        result = None
        try:
            result = requests.post(url=self.dao_backend_url)
        except Exception as e:
            print('DeFi360TokenBalances request error: ', e)

        if pydash.get(result.json(), 'data') != 'success':
            print('DeFi360TokenBalances result error: ', result.json())

        print('DeFi360TokenBalances request success!!!')

    def airflow_steps(self):
        return [
            self.post_dao_backend
        ]

    @staticmethod
    def airflow_dag_params():
        dag_params = {
            "dag_id": "DeFi360_token_balances_dag",
            "catchup": False,
            "schedule_interval": '35 6 * * *',
            "description": "DeFi360_token_balances_dag",
            "default_args": {
                'owner': 'airflow',
                'depends_on_past': False,
                'retries': 1,
                'retry_delay': datetime.timedelta(minutes=5),
                'start_date': datetime.datetime(2022, 1, 1)
            },
            "dagrun_timeout": datetime.timedelta(days=30)
        }
        print('dag_params', dag_params)
        return dag_params


deFi360TokenBalances = DeFi360TokenBalances()

DAG = BuildDAG().build_dag_with_ops(
    dag_params=deFi360TokenBalances.airflow_dag_params(),
    ops=deFi360TokenBalances.airflow_steps()
)
