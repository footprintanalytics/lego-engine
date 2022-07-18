from .base_basic_validate import BaseBasicValidate
import re
import pydash
from utils.query_bigquery import query_bigquery


class AbnormalNumberValidate(BaseBasicValidate):
    validate_type = 'basic'
    validate_name = 'abnormal_number'
    desc = '数值相关字段不能有异常大的值'
    slack_warn = False
    switch_validate = True
    validate_args = {
        "max": 100000000
    }

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
        token_symbol = pydash.get(self_data, 'data.symbol.0', [])
        token_gen_symbol = pydash.get(self_data, 'data.a_symbol.0', [])
        token_pay_symbol = pydash.get(self_data, 'data.b_symbol.0', [])
        symbol = list({*token_symbol, *token_pay_symbol, *token_gen_symbol})

        token_address = pydash.get(self_data, 'data.address.0', [])
        token_gen_address = pydash.get(self_data, 'data.a_address.0', [])
        token_pay_address = pydash.get(self_data, 'data.b_address.0', [])
        address = list({*token_address, *token_pay_address, *token_gen_address})

        return {
            "result_code": 0 if invalidate_count == 0 else 1,
            "result_message": '数值异常值校验成功' if invalidate_count == 0 else '数值异常值校验失败',
            "validate_data": {
                "validate_address": address,
                "validate_symbol": symbol,
                "validate_table": self.validate_table,
                "invalidate_address_count": invalidate_count,
                "sql": pydash.get(self_data, 'sql', 0)
            }
        }

    def get_self_sql(self):
        return ''
