from inner_validate.validate_cls.base_validate import BaseValidate
from utils.query_bigquery import query_bigquery


class AddressFormatValid(BaseValidate):
    valid_rule_name = 'VALID_LEGAL_ADDRESS_FORMAT'

    def validate_result(self, valid_sql: str, fields_info: list):
        df = query_bigquery(valid_sql)
        result = []

        for info in fields_info:
            len_str = len(
                df[~((df[info.get('field')].str.len() == 42) & (df[info.get('field')].str.startswith('0x')))]
            )
            if len_str > 0:
                result += [{
                    'rule_name': self.valid_rule_name,
                    'field': info.get('field'),
                    'num': len_str
                }]
        return result
