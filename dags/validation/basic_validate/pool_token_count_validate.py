from .base_basic_validate import BaseBasicValidate
import pydash
from utils.query_bigquery import query_bigquery


class PoolTokenCountValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'token_a_b'
    desc = '池子token数'
    slack_warn = False
    project_token_count_limit_map = {
        'balancer': {
            '1': 8,
            '2': 500
        },
        'curve': {
            '1': 500,
            '2': 500
        }
    }
    token_count_limit_map = {
        '1': 2,
        '2': 2,
        '3': 2,
    }

    def __init__(self, cfg):
        super().__init__(cfg)
        validate_table = cfg["validate_table"]
        self.validate_table = [validate_table]
        if 'token_count_limit_map' in cfg:
            self.token_count_limit_map = cfg['token_count_limit_map']
        elif self.project.lower() in self.project_token_count_limit_map:
            self.token_count_limit_map = self.project_token_count_limit_map[self.project.lower()]

    def get_self_data(self):
        sql = self.get_self_sql()
        data = query_bigquery(sql)
        return {
            "data": data.to_dict(orient='record'),
            "sql": sql
        }

    def check_data(self, self_data: dict):
        invalidate_count = len(self_data['data'])
        return {
            "result_code": 0 if invalidate_count == 0 else 1,
            "result_message": 'pool token数正常' if invalidate_count == 0 else 'pool token数异常',
            "validate_data": {
                "invalidate_ls": self_data['data'][:5],
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }

    def get_self_sql(self):

        filter_sql_ls = []
        for version in self.token_count_limit_map:
            filter_sql_ls.append(f"(version = '{version}' and token_count > {self.token_count_limit_map[version]})")
        filter_sql = ' or '.join(filter_sql_ls)
        return """
        select project,exchange_address,count(token_address) as token_count,version from (
            select exchange_address,token_address,project,version from {table} group by exchange_address,token_address,project,version
        ) group by exchange_address,project,version having
        {filter_sql}
        """.format(table=self.validate_table[0], filter_sql=filter_sql)
