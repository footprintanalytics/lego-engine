from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class FieldNotNullValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'DeFi360_field_not_null_validate'
    desc = '字段不能为空'
    slack_warn = False
    switch_validate = True


    def get_self_sql(self):
        null_field = []
        for field in self.validate_field:
            null_field.append(
                f"{field} is null"
            )
        null_field = ' or '.join(null_field)

        sql = """
            select count(1) as count from `{validate_table}` where ({null_field})
        """.format(
            validate_table=self.validate_table[0],
            null_field=null_field
        )
        return sql
