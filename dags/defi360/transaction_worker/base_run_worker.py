import requests


class BaseRunWorker:
    def __init__(self, chain):
        self.chain = chain

    @classmethod
    def _query_to_hasura(cls, query: str, variables: dict = {}, extract=False):
        print(f'query_to_hasura.query: {query}')
        request = requests.post(
            'https://hasura-prod.internal.footprint.network/v1/graphql',
            json={"query": query, "variables": variables},
        )
        assert request.ok, f"Failed with code {request.status_code}"
        res = request.json()
        print(f'query_to_hasura.res==={res}')
        return res

    @property
    def worker_query(self) -> str:
        pass

    def get_worker(self):
        res = self._query_to_hasura(self.worker_query)
        return res

    def run_worker(self, worker_info):
        pass

    def verify(self, worker_info):
        pass

    def exec(self):
        worker_info = self.get_worker()
        return self.run_worker(worker_info)
