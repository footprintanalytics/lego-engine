from inner_validate.validate_cls.base_validate import BaseValidate
from utils.query_bigquery import query_bigquery


class LessThanOrEqZeroValidate(BaseValidate):
    valid_rule_name = 'FIELD_LEGAL_LESS_THAN_OR_EQ_ZERO'

    def validate_result(self, valid_sql: str, fields_info: list):
        df = query_bigquery(valid_sql)
        result = [
            {'rule_name': self.valid_rule_name, 'field': info.get('field'), 'num': len(df[df[info.get('field')] <= 0])}
            for info in fields_info if not
            df[df[info.get('field')] <= 0].empty]
        return result
