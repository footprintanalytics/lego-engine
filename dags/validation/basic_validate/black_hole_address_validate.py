from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class BlackHoleAddressValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'black_hole_address_format_validate'
    desc = '黑洞地址检测：0x000'
    slack_warn = False

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
            "result_message": '黑洞地址格式校验成功' if invalidate_count == 0 else '黑洞地址格式校验失败',
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
                f"(lower({field}) = '0x0000000000000000000000000000000000000000')"
            )
        address_sql = ' or '.join(address_sql)

        sql = """
            select count(1) as count from `{validate_table}` where ({address_sql})
        """.format(
            validate_table=self.validate_table[0],
            address_sql=address_sql
        )
        return sql
