from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class NegativeNumberValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'DeFi360_negative_number_validate'
    desc = '数值相关字段不应该小于等于0'
    slack_warn = False
    switch_validate = True
    validate_args = {
        # 最小值
        "min": 0
    }

    def get_self_sql(self):
        field_sql = []
        for field in self.validate_field:
            field_sql.append(f"{field} <= {self.validate_args['min']}")
        field_sql = ' or '.join(field_sql)

        sql = """
            select count(1) as count from `{validate_table}` where ({field_sql})
        """.format(
            validate_table=self.validate_table[0],
            field_sql=field_sql
        )
        return sql
