from .base_basic_validate import BaseBasicValidate
import pydash
from utils.query_bigquery import query_bigquery


class TokenABValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'token_a_b'
    desc = 'token a/b 不能相同'
    slack_warn = False

    def __init__(self, cfg):
        super().__init__(cfg)
        validate_table = cfg["validate_table"]
        self.validate_table = [validate_table]

    def get_self_data(self):
        sql = self.get_self_sql()
        data = query_bigquery(sql)
        return {
            "data": data.to_dict(orient='dict'),
            "sql": sql
        }

    def check_data(self, self_data: dict):
        invalidate_count = pydash.get(self_data, 'data.count.0', 0)
        return {
            "result_code": 0 if invalidate_count == 0 else 1,
            "result_message": 'token a/b正常' if invalidate_count == 0 else 'token a/b有重复',
            "validate_data": {
                "invalidate_address_count": invalidate_count,
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }

    def get_self_sql(self):
        return """
            select count(1) as count from {table}
            where token_a_address = token_b_address
        """.format(table=self.validate_table[0])
