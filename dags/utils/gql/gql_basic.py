import requests
import json
from config import project_config

hasura_url = project_config.hasura_host + '/v1/graphql'


class GraphqlBasic(object):

    def fetchGraphql(self, operationsDoc: str, operationName: str, variables):
        body = {
            'query': operationsDoc,
            'variables': variables,
            'operationName': operationName
        }
        result = requests.post(url=hasura_url, data=json.dumps(body))
        print(result.text)
        return result
