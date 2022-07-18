from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class AddressFormatValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'address_format_validate'
    desc = '地址相关字段格式应该正确：1、0x开头，2、长度42，3、全部小写'
    slack_warn = False
    switch_validate = True
    validate_args = {
        # 长度为42
        "length": 42,
        # 匹配不以0x开头
        "regex_0x": "^[^0x]",
        # 匹配大写
        "regex_capital": "[A-Z]+"
    }

    def __init__(self, cfg):
        super().__init__(cfg)
        validate_field = cfg["validate_field"]
        validate_table = cfg["validate_table"]

        self.validate_field = validate_field
        self.validate_table = [validate_table]

    def get_self_data(self):
        sql = self.get_address_format_sql()
        data = query_bigquery(sql)
        return {
            "data": data.to_dict(orient='dict'),
            "sql": sql
        }

    def check_data(self, self_data: dict):
        invalidate_count = pydash.get(self_data, 'data.count.0', 0)
        return {
            "result_code": 0 if invalidate_count == 0 else 1,
            "result_message": '地址格式校验成功' if invalidate_count == 0 else '地址格式校验失败',
            "validate_data": {
                "validate_field": self.validate_field,
                "validate_table": self.validate_table,
                "invalidate_address_count": invalidate_count,
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }

    def get_address_format_sql(self):
        address_sql = []
        for field in self.validate_field:
            address_sql.append(
                f"(lower({field}) not in ('bnb', 'eth', 'matic') and (REGEXP_CONTAINS({field},"
                f"r'{self.validate_args['regex_capital']}|{self.validate_args['regex_0x']}')"
                f" or length({field}) != {self.validate_args['length']}))"
            )
        address_sql = ' or '.join(address_sql)

        sql = """
            select count(1) as count from `{validate_table}` where ({address_sql})
        """.format(
            validate_table=self.validate_table[0],
            address_sql=address_sql
        )
        return sql
