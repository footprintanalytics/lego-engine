import requests

from defi360.transaction_worker.base_run_worker import BaseRunWorker


class PoolInfo(BaseRunWorker):
    @property
    def worker_query(self) -> str:
        return f"""
    query MyQuery {{
  indicator_pool(where: {{pool_sql: {{_is_null: false}}, chain: {{_eq: "{self.chain}"}}}}) {{
    chain
    defi_category
    deposit_contract_address_list
    id
    name
    pool_sql
    protocol_id
    return_token_address_list
    token_address_list
    user_id
    version
  }}
}}
"""

    def run_worker(self, query_result):
        for msg in query_result['data']['indicator_pool']:
            try:
                res = requests.post(
                    'https://us-east4-xed-project-237404.cloudfunctions.net/pool_info_on_blockchain_http',
                    json={
                        'data': {
                            'data': msg,
                            "id": msg['id'],
                            'get_history': False,
                            "table_name": "cash_flow_sql",
                        }
                    },
                    timeout=1000
                )
                print(f'res=={res.text}')
            except Exception as e:
                print(f'error message === {e}')
                print(f'error message === {msg}')

    def parse_pool_info(self):
        # airflow fix 兼容函数名不能相同的bug.
        self.exec()
