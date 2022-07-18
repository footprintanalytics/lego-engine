from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class BlackHoleAddressValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'DeFi360_black_hole_address_format_validate'
    desc = '不能有黑洞地址：0x000..00'
    slack_warn = False

    def get_self_sql(self):
        field_sql = []
        for field in self.validate_field:
            field_sql.append(
                f"lower({field})='0x0000000000000000000000000000000000000000'"
            )
        field_sql = ' or '.join(field_sql)

        sql = """
            select count(1) as count from `{validate_table}` where ({field_sql})
        """.format(
            validate_table=self.validate_table[0],
            field_sql=field_sql
        )
        return sql
