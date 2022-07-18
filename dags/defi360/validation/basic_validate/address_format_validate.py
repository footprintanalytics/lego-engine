from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class AddressFormatValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'DeFi360_address_format_validate'
    desc = '地址相关字段格式应该正确：[^(0x)?[0-9a-fA-F]{40}$]'
    slack_warn = False
    switch_validate = True
    validate_args = {
        # 正则匹配
        "regex": "^[^(0x)?[0-9a-fA-F]{40}$]"
    }

    def get_self_sql(self):
        address_sql = []
        for field in self.validate_field:
            address_sql.append(
                f"REGEXP_CONTAINS({field},r'{self.validate_args['regex']}')"
            )
        address_sql = ' or '.join(address_sql)

        sql = """
            select count(1) as count from `{validate_table}` where ({address_sql})
        """.format(
            validate_table=self.validate_table[0],
            address_sql=address_sql
        )
        return sql
