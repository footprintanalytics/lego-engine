import datetime

import requests

from defi360.transaction_worker.base_run_worker import BaseRunWorker
from utils.date_util import DateUtil


class PoolParseEvent(BaseRunWorker):
    @property
    def worker_query(self) -> str:
        return f"""
    query MyQuery {{
      indicator_event(where: {{chain: {{_eq: "{self.chain}"}}}}) {{
        event
        id
        protocol_id
        chain
        contract_address_list
        contract_address_sql
        user_id
      }}
    }}
    """

    def run_worker(self, query_result):
        for msg in query_result['data']['indicator_event']:
            try:
                res = requests.post(
                    'https://us-east4-xed-project-237404.cloudfunctions.net/pool_parse_event_on_blockchain_http',
                    json={
                        'data': {
                            'data': msg,
                            'get_history': False,
                            'end_date': (DateUtil.utc_start_of_date() - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                            "id": msg['id'],
                            "table_name": "cash_flow_sql",
                        }
                    },
                    timeout=1000
                )
                print(f'res=={res.text}')
            except Exception as e:
                print(f'error message === {e}')
                print(f'error message === {msg}')

    def parse_event(self):
        # airflow fix 兼容函数名不能相同的bug.
        self.exec()
