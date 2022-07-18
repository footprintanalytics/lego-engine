from duneanalytics import DuneAnalytics

class DuneQuery:
    def __init__(self, user_name='USER_NAME', user_password='USER_PASSWORD'):
        self.dune = DuneAnalytics(user_name, user_password)
        self.dune.login()
        self.dune.fetch_auth_token()

    def query_result(self, query_id):
        result_id = self.dune.query_result_id(query_id=query_id)
        data = self.dune.query_result(result_id)
        return data