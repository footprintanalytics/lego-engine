from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class NegativeNumberValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'negative_number_validate'
    desc = '数值相关字段不应该有负数'
    slack_warn = False
    switch_validate = True
    validate_args = {
        # 最小值
        "min": 0
    }

    def __init__(self, cfg):
        super().__init__(cfg)
        validate_field = cfg["validate_field"]
        validate_table = cfg["validate_table"]

        self.validate_field = validate_field
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
            "result_message": '数值负数校验成功' if invalidate_count == 0 else '数值负数校验失败',
            "validate_data": {
                "validate_field": self.validate_field,
                "validate_table": self.validate_table,
                "invalidate_address_count": invalidate_count,
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }

    def get_self_sql(self):
        address_sql = []
        for field in self.validate_field:
            address_sql.append(f" {field} < {self.validate_args['min']} ")
        address_sql = ' or '.join(address_sql)

        sql = """
            select count(1) as count from `{validate_table}` where ({address_sql})
        """.format(
            validate_table=self.validate_table[0],
            address_sql=address_sql
        )
        return sql
